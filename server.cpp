// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

// c++
#include <vector>
#include <string>

// proj
#include "hashtable.h"
#include "zset.h"
#include "common.h"
#include "list.h"

static void msg(const char* msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char* msg) {
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char* msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static uint64_t get_monotonic_msec() {
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000 + tv.tv_nsec / 1000 / 1000;
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if(errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }    
}

const size_t k_max_msg = 32 << 20; // likely larger than the kernel buffer

typedef std::vector<uint8_t> Buffer;

// append to the back
static void buf_append(Buffer& buf, const uint8_t* data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

// remove from the front
static void buf_consume(Buffer& buf, size_t len) {
    buf.erase(buf.begin(), buf.begin() + len);
}

struct Conn {
    int fd = -1;
    // application's intention for the event loop
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    // buffered input and output
    Buffer incoming;  // data to be parsed by the application
    Buffer outgoing;  // responses generated by the application
    // timer
    uint64_t last_active_ms = 0;
    DList idle_node;
};

// global states
static struct {
    HMap db;    // top-level hashtable
    // a map of all client connections, keyed by fd
    std::vector<Conn*> fd2conn;
    // timers for idle connections
    DList idle_list;
} g_data;

// application callback when the listening socket is ready
static Conn* handle_accept(int fd) {
    // accept
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int conn_fd = accept(fd, (struct sockaddr*)&client_addr, &socklen);
    if (conn_fd < 0) {
        msg_errno("accept() error");
        return NULL;
    }

    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
            (ip >> 0) & 255, (ip >> 8) & 255, (ip >> 16) & 255, (ip >> 24) & 255,
            ntohs(client_addr.sin_port));
    
    // set the new connection fd to non blocking mode
    fd_set_nb(conn_fd);

    // create a 'struct Conn'
    Conn* conn = new Conn();
    conn->fd = conn_fd;
    conn->want_read = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_data.idle_list, &conn->idle_node);

    // put it into the map
    if (g_data.fd2conn.size() <= (size_t)conn->fd) {
        g_data.fd2conn.resize(conn->fd + 1);
    }

    assert(!g_data.fd2conn[conn->fd]);
    g_data.fd2conn[conn->fd] = conn;
    return 0;
}

static void conn_destroy(Conn* conn) {
    (void)close(conn->fd);
    g_data.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);
    delete conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_u32(const uint8_t* &cur, const uint8_t* end, uint32_t& out) {
    if (cur + 4 > end) {
        return false;
    }
    memcpy(&out, cur, 4);
    cur+=4;
    return true;
}

static bool read_str(const uint8_t* &cur, const uint8_t* end, size_t n, std::string &out) {
    if (cur + n > end) {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+

static int32_t parse_req(const uint8_t* data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr)){
        return -1;
    }
    if (nstr > k_max_args){
        return -1; // safety limit
    }

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) {
            return -1;
        }        
    }
    if (data != end) {
        return -1; // trailing garbage
    }
    return 0;
}

// error code for TAG_ERR
enum {
    ERR_UNKNOWN = 1,     // unknow command
    ERR_TOO_BIG = 2,     // response too big
    ERR_BAD_TYP = 3,     // unexpected value type
    ERR_BAD_ARG = 4,     // bad arguments 
};

// data types of serialized data
enum {
    TAG_NIL = 0,    // nil
    TAG_ERR = 1,    // error code + msg
    TAG_STR = 2,    // string
    TAG_INT = 3,    // int64
    TAG_DBL = 4,    // double
    TAG_ARR = 5,    // array
};

// help functions for serialization
static void buf_append_u8(Buffer &buf, uint8_t data) {
    buf.push_back(data);
}

static void buf_append_u32(Buffer &buf, uint32_t data) {
    buf_append(buf, (const uint8_t*)&data, 4);
}

static void buf_append_i64(Buffer &buf, int64_t data) {
    buf_append(buf, (const uint8_t*)&data, 8);
}

static void buf_append_dbl(Buffer &buf, double data) {
    buf_append(buf, (const uint8_t*)&data, 8);
}

// append serialized data types to the block
static void out_nil(Buffer &out) {
    buf_append_u8(out, TAG_NIL);
}

static void out_str(Buffer &out, const char* s, size_t size) {
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t*)s, size);
}

static void out_int(Buffer &out, int64_t val) {
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}

static void out_dbl(Buffer &out, double val) {
    buf_append_u8(out, TAG_DBL);
    buf_append_dbl(out, val);
}

static void out_err(Buffer &out, uint32_t code, const std::string &msg) {
    buf_append_u8(out, TAG_ERR);
    buf_append_u32(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t*)msg.data(), msg.size());
}

static void out_arr(Buffer &out, uint32_t n) {
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}

static size_t out_begin_arr(Buffer &out) {
    out.push_back(TAG_ARR);
    buf_append_u32(out, 0);     // filled by out_end_arr()
    return out.size() - 4;      // the `ctx` arg
}

static void out_end_arr(Buffer &out, size_t ctx, uint32_t n) {
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}

// value types
enum {
    T_INIT  = 0,
    T_STR   = 1,    // string
    T_ZSET  = 2,    // sorted set
};

// KV pair for the top-level hashtable
struct Entry {
    struct HNode node;  // hashtable node
    std::string key;
    // value
    uint32_t type = 0;
    // one of the following
    std::string str;
    ZSet zset;
};

static Entry* entry_new(uint32_t type) {
    Entry* ent = new Entry();
    ent->type = type;
    return ent;
}

static Entry* entry_del(Entry* ent) {
    if (ent->type == T_ZSET) {
        zset_clear(&ent->zset);
    }
    delete ent;
}

struct LookupKey {
    struct HNode node; // hashtable node
    std::string key;
};

// equality comparison for the top-level hashtable
static bool entry_eq(HNode* node, HNode* key) {
    struct Entry* ent = container_of(node, struct Entry, node);
    struct LookupKey* keydata = container_of(key, struct LookupKey, node);
    return ent->key == keydata->key;
}

static void do_get(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    // hashtable lookup
    HNode* node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_nil(out);
    }
    // copy the value
    Entry* ent = container_of(node, Entry, node);
    if (ent->type != T_STR) {
        return out_err(out, ERR_BAD_TYP, "not a string value");
    }
    return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    // hashtable lookup
    HNode* node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if(node) {
        // found, update the value
        Entry* ent = container_of(node, Entry, node);
        if (ent->type != T_STR) {
            return out_err(out, ERR_BAD_TYP, "a non-string value exists");
        }
        ent->str.swap(cmd[2]);
    } else {
        // not found, allocate and insert a new pair
        Entry* ent = entry_new(T_STR);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g_data.db, &ent->node);
    }
    return out_nil(out);
}

static void do_del(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    // hashtable delete
    HNode* node = hm_delete(&g_data.db, &key.node, &entry_eq);
    if(node) {  // deallocate the pair
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}

static bool cb_keys(HNode *node, void *arg) {
    Buffer &out = *(Buffer*)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &, Buffer &out) {
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void*)&out);
}

static bool str2dbl(const std::string &s, double &out) {
    char* endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}


static bool str2int(const std::string &s, int64_t &out) {
    char* endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, Buffer &out) {
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_BAD_ARG, "expect float");
    }

    // look up or create the zset
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode* hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry* ent = NULL;
    if (!hnode) {   // insert a new key
        ent = entry_new(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    } else {        // check the existing key
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET) {
            return out_err(out, ERR_BAD_TYP, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added  = zset_insert(&ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

static const ZSet k_empty_zset;

static ZSet* expect_zset(std::string &s) {
    LookupKey key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode* hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode) {   // a non-existent key is treated as an empty zset
        return (ZSet*)&k_empty_zset;
    }
    Entry* ent = container_of(hnode, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, Buffer &out) {
    ZSet* zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode* znode = zset_lookup(zset, name.data(), name.size());
    if (znode) {
        zset_delete(zset, znode);
    }
    return out_int(out, znode ? 1 : 0);
}

// zscore zset nam
static void do_zscore(std::vector<std::string> &cmd, Buffer &out) {
    ZSet* zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode* znode = zset_lookup(zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, Buffer &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_BAD_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset) || !str2int(cmd[5], limit)) {
        return out_err(out, ERR_BAD_ARG, "expect int");
    }

    // get the zset
    ZSet* zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    // seek to the key
    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode* znode = zset_seekge(zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    //output
    size_t ctx = out_begin_arr(out);
    int64_t n = 0;
    while (znode && n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n += 2;
    }
    out_end_arr(out, ctx, (uint32_t)n);
}

static void do_request(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        return do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        return do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        return do_del(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out);
    } else if (cmd.size() == 4 && cmd[0] == "zadd") {
        return do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zrem") {
        return do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zscore") {
        return do_zscore(cmd, out);
    }  else if (cmd.size() == 6 && cmd[0] == "zquery") {
        return do_zquery(cmd, out);
    } else {
        return out_err(out, ERR_UNKNOWN, "unknown command.");
    }
}

static void response_begin(Buffer &out, size_t *header) {
    *header = out.size();   // message header position
    buf_append_u32(out, 0); // reserve 4 bytes for the message length
}

static size_t response_size(Buffer &out, size_t header) {
    return out.size() - header - 4;
}

static void response_end(Buffer &out, size_t header) {
    size_t msg_size = response_size(out, header);
    if (msg_size > k_max_msg) {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response too big.");
        msg_size = response_size(out, header);
    }
    // message header
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[header], &len, 4);
}

// process 1 request if there is enough data
static bool try_one_request(Conn* conn) {
    // try to parse the protocol: message header
    if (conn->incoming.size() < 4) {
        return false; // not enough data, want more read
    }

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        msg("message too long");
        conn->want_close = true;
        return false; // want close
    }

    // message body
    if (conn->incoming.size() < 4 + len) {
        return false; // not enough data, want more read
    }
    const uint8_t* request = &conn->incoming[4];

    // got one request, do some application logic
    std::vector<std::string> cmd;
    if (parse_req(request, len, cmd) < 0) {
        msg("bad request");
        conn->want_close = true;
        return false; // want close
    }

    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);
    do_request(cmd, conn->outgoing);
    response_end(conn->outgoing, header_pos);

    // application logic done! remove the request message
    buf_consume(conn->incoming, 4 + len);
    // Q. Why not just empty the buffer? see the explanation of "pipelining"
    return true; // success
}

// application callback when the socket is writable
static void handle_write(Conn* conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return; // EAGAIN means actually not ready
    }

    // handle IO error
    if (rv < 0) {
        msg_errno("write() error");
        conn->want_close = true;
        return; // want close
    }

    // remove written data from 'outgoing'
    buf_consume(conn->outgoing, (size_t)rv);

    // update the readiness intention
    if (conn->outgoing.size() == 0) {    // all data written
        conn->want_read = true;
        conn->want_write = false;
    }   // else: want write
}

// application callback when the socket is readable
static void handle_read(Conn* conn) {
    // read some data
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return; // EAGAIN means actually not ready
    }

    // handle IO error
    if (rv < 0) {
        msg_errno("read() error");
        conn->want_close = true;
        return; // want close
    }
    
    // handle EOF
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return; // want close
    }

    // got some new data
    buf_append(conn->incoming, buf, (size_t)rv);

    // parse requests and generate responses
    // try_one_request(conn); wrong!
    while (try_one_request(conn)) {}
    // Why calling this in a loop? See the explanation of "pipelining"

    // update the readiness intention
    if (conn->outgoing.size() > 0) {
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration
        return handle_write(conn);
    }   // else: want read
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

static int32_t next_timer_ms() {
    if (dlist_empty(&g_data.idle_list)) {
        return -1;  // no timers, no timeout
    }

    uint64_t now_ms = get_monotonic_msec();
    Conn* conn = container_of(g_data.idle_list.next, Conn, idle_node);
    uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
    if (next_ms <= now_ms) {
        return 0;   // missed?
    }
    return (int32_t)(next_ms - now_ms);
}

static void process_timers() {
    uint64_t now_ms = get_monotonic_msec();
    while (!dlist_empty(&g_data.idle_list)) {
        Conn* conn = container_of(g_data.idle_list.next, Conn, idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if (next_ms >= now_ms) {
            break;  // not expired
        }

        fprintf(stderr, "removing idle connection: %d\n", conn->fd);
        conn_destroy(conn);
    }
}

int main() {
    // initialisation
    dlist_init(&g_data.idle_list);

    // Create a socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    // needed for reusing the same address on restart
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind the socket to an address
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    int rv = bind(fd, (const sockaddr*)&addr, sizeof(addr));
    if (rv < 0) {
        die("bind()");
    }

    // set the listen fd to non-blocking mode
    fd_set_nb(fd);

    // listen for incoming connections
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    // the event loop
    std::vector<struct pollfd> poll_args;
    while (true) {
        // prepare the arguments of the poll() call
        poll_args.clear();

        // put the listening socket in the first position
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);

        // the rest are connection sockets
        // Initially this might be empty
        for (Conn* conn: g_data.fd2conn) {
            if (!conn) {
                continue;
            }
            // always poll for error
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // poll() flags from the application's intent
            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // wait for readiness
        int32_t timeout_ms = next_timer_ms();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
        if (rv < 0 && errno == EINTR) {
            continue;
        }
        if (rv < 0) {
            die("poll()");
        }

        // handle the listening socket
        if (poll_args[0].revents) {
            handle_accept(fd);
        }

        // handle connection sockets
        for (size_t i = 1; i < poll_args.size(); ++i) {
            uint32_t ready = poll_args[i].revents;
            if (ready == 0) {
                continue;
            }

            Conn* conn = g_data.fd2conn[poll_args[i].fd];

            // Update the idle timer by moving conn to the end of the list
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list, &conn->idle_node);

            // handle IO
            if (ready & POLLIN) {
                assert(conn->want_read);
                handle_read(conn); // application logic
            }

            if (ready & POLLOUT) {
                assert(conn->want_write);
                handle_write(conn); // application logic
            }

            // close the socket if socket error or application logic
            if ((ready & POLLERR) || conn->want_close) {
                conn_destroy(conn);
            }
        } // for each connection sockets

        // handle timers
        process_timers();
    } // the event loop
    
    return 0;
}