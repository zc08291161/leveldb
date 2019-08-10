#ifndef ART_KEY_PATH_IMPL_H
#define ART_KEY_PATH_IMPL_H

#include "art_path.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : init key path.
* key_path                  : pointer to key path
*/
void init_key_path(
    IO art_key_path_t *key_path);


/*
* description               : init key path operations.
* key_path                  : pointer to key path
* ops                       : operation set
*/
void init_key_path_ops(
    IO art_key_path_t *key_path,
    IN art_path_operations_t *ops);


/*
* description               : assign internal operation set for key path
* key_path                  : pointer to key path
* ops                       : internal operation set
*/
void assign_key_path_internal_ops(
    IO art_key_path_t *key_path,
    IN artree_operations_t *ops);


/*
* description               : add key to key path.
* key_path                  : pointer to key path
* key                       : pointer to key
* key_len                   : key length
*/
static inline void push_key_into_key_path(
    IN art_key_path_t *key_path,
    IN uint8_t *key,
    IN uint32_t key_len)
{
    ART_MEMCPY(key_path->prefix + key_path->prefix_len, key, key_len);
    key_path->prefix_len += key_len;
}


/*
* description               : remove key from key path.
* key_path                  : pointer to key path
* key_len                   : key length
*/
static inline void pop_key_from_key_path(
    IN art_key_path_t *key_path,
    IN uint32_t key_len)
{
    key_path->prefix_len -= key_len;
	//ART_MEMSET(key_path->prefix + key_path->prefix_len, 0, key_len);
}


/*
* description               : check key path if empty. 
* key_path                  : pointer to key path
* return                    : true if empty, else return false.
*/
static inline bool check_key_path_empty(
    IN art_key_path_t *key_path)
{
    return 0 == key_path->path_index;
}


/*
* description               : push node to path last, prev node is its parent
* key_path                  : pointer to key path
* node                      : pushed node
* pos_in_parent             : its pos in parent node
*/
void push_node_into_key_path(
    IO art_key_path_t *key_path,
    IN void *node,
    IN uint32_t pos_in_parent);


/*
* description               : pop last node from key path.
* key_path                  : pointer to key path
* return                    : last node in path
*/
void *pop_node_from_key_path(
    IN art_key_path_t *key_path);

/*
* description               : get last node info from key path. 
* key_path                  : pointer to key path
* pos_in_parent             : node position in its parent'
* child_pos                 : current handling child pos
* parent_idx                : parent index in path
* return                    : last node if ok, else return NULL.
*/
void *get_last_node_in_key_path(
    IN art_key_path_t *key_path,
    OUT uint32_t *pos_in_parent,
    OUT uint32_t *child_pos,
    OUT uint32_t *parent_idx);


/*
* description               : set last node current handling child pos. 
* key_path                  : pointer to key path
* child_pos                 : current handling child pos
*/
static inline void set_last_child_pos_in_key_path(
    IO art_key_path_t *key_path,
    IN uint32_t child_pos)
{
    if(check_key_path_empty(key_path))
    {
        return;
    }

    key_path->path[key_path->path_index - 1].child_pos = child_pos;
}


/*
* description               : get last node current handling child pos. 
* key_path                  : pointer to key path
* return                    : current handling child pos
*/
static inline uint32_t get_last_node_child_pos_in_key_path(
    IN art_key_path_t *key_path)
{
    if(check_key_path_empty(key_path))
    {
        return ART_INVALID_CHILD_POS;
    }
    
    return key_path->path[key_path->path_index - 1].child_pos;
}


/*
* description               : get prefix len in key path. 
* key_path                  : pointer to key path
*/
static inline uint32_t get_prefix_len_in_key_path(
    IN art_key_path_t *key_path)
{
    return key_path->prefix_len;
}

/*
* description               : clear prefix in key path. 
* key_path                  : pointer to key path
*/
static inline void clear_key_path_prefix(
    IN art_key_path_t *key_path)
{
    ART_MEMSET(key_path->prefix, 0, key_path->prefix_len);
    key_path->prefix_len = 0;
}

void set_node_release_status_key_path(
    IO art_key_path_t *key_path,
    IN bool is_need_release,
    IN void* buff_node);



void get_node_release_status_key_path(
    IO art_key_path_t *key_path,
    OUT bool* is_need_release,
    OUT void** buff_node);



#ifdef __cplusplus
}
#endif
#endif


