#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

static void msg(const char* msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char* msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

const size_t k_max_msg = 4096;

static int32_t read_full(int fd, char* buf, size_t n) {
    while (n > 0) {
        size_t rv = read(fd, buf, n);
        if (rv <= 0 ) {
            return -1; // error or unexpected EOF
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }

    return 0;
}

static int32_t write_all(int fd, const char* buf, size_t n) {
    while (n > 0) {
        size_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1; // error
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }

    return 0;
}

static int32_t one_request(int conn_fd) {
    // 4 bytes header
    char rbuf[4 + k_max_msg];
    errno = 0;
    int32_t err = read_full(conn_fd, rbuf, 4);
    if (err) {
        msg(errno == 0 ? "EOF" : "read() error");
        return err;
    }

    // request length
    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > k_max_msg) {
        msg("message too long");
        return -1;
    }

    // request body
    err = read_full(conn_fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }

    // do something
    fprintf(stderr, "client says: %.*s\n", len, &rbuf[4]);

    // reply using the same protocol
    const char reply[] = "world";
    char wbuf[4 + sizeof(reply)];
    len = (uint32_t)strlen(reply);
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], reply, len);
    return write_all(conn_fd, wbuf, 4 + len);
}

int main() {
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

    // listen for incoming connections
    rv = listen(fd, SOMAXCONN);
    if (rv < 0) {
        die("listen()");
    }

    while (true) {
        // accept a connection
        struct sockaddr_in client_addr = {};
        socklen_t client_addr_len = sizeof(client_addr);
        int conn_fd = accept(fd, (struct sockaddr*)&client_addr, &client_addr_len);
        if (conn_fd < 0) {
            continue;
        }

        while (true) {
            // server serves only 1 client connectionat once
            int32_t err = one_request(conn_fd);
            if (err) {
                break;
            }
        }
        
        close(conn_fd);
    }
    
    return 0;
}