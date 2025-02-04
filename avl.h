#pragma once

#include <stdint.h>
#include <stddef.h>

struct AVLNode {
    AVLNode* parent = NULL;
    AVLNode* left = NULL;
    AVLNode* right = NULL;
    uint32_t height = 0;    // subtree height
    uint32_t cnt = 0;       // subtree size
};

inline void avl_init(AVLNode* node) {
    node->parent = NULL;
    node->left = NULL;
    node->right = NULL;
    node->height = 1;
    node->cnt = 1;
}

// helpers
inline uint32_t avl_height(AVLNode* node) {
    return node ? node->height : 0;
}

inline uint32_t avl_cnt(AVLNode* node) {
    return node ? node->cnt : 0;
}

// API
AVLNode* avl_fix(AVLNode* node);
AVLNode* avl_del(AVLNode* node);