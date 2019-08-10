#ifndef ART_SNAP_PATH_IMPL_H
#define ART_SNAP_PATH_IMPL_H

#include "art_path.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : init snap path.
* snap_path                 : pointer to snap path
*/
void init_snap_path(
    IO art_snap_path_t *snap_path);


/*
* description               : init snap path operations.
* snap_path                 : pointer to snap path
* ops                       : operation set
*/
void init_snap_path_ops(
    IO art_snap_path_t *snap_path,
    IN art_path_operations_t *ops);


/*
* description               : assign internal operation set for key path
* snap_path                 : pointer to snap path
* ops                       : internal operation set
*/
void assign_snap_path_internal_ops(
    IO art_snap_path_t *snap_path,
    IN artree_operations_t *ops);


/*
* description               : add key to snap path.
* snap_path                 : pointer to snap path
* key                       : pointer to key
* key_len                   : key length
*/
static inline void push_key_into_snap_path(
    IN art_snap_path_t *snap_path,
    IN uint8_t *key,
    IN uint32_t key_len)
{
    ART_MEMCPY(snap_path->prefix + snap_path->prefix_len, key, key_len);
    snap_path->prefix_len += key_len;
}


/*
* description               : remove key from snap path.
* snap_path                 : pointer to snap path
* key_len                   : key length
*/
static inline void pop_key_from_snap_path(
    IN art_snap_path_t *snap_path,
    IN uint32_t key_len)
{
    snap_path->prefix_len -= key_len;
	ART_MEMSET(snap_path->prefix + snap_path->prefix_len, 0, key_len);
}


/*
* description               : check snap path if empty. 
* snap_path                 : pointer to snap path
* return                    : true if empty, else return false.
*/
static inline bool check_snap_path_empty(
    IN art_snap_path_t *snap_path)
{
    return 0 == snap_path->path_index;
}


/*
* description               : push node to path last, prev node is its parent
* snap_path                 : pointer to snap path
* node                      : pushed node
* pos_in_parent             : its pos in parent node
*/
void push_node_into_snap_path(
    IO art_snap_path_t *snap_path,
    IN void *node,
    IN uint32_t pos_in_parent);


/*
* description               : pop last node from snap path.
* snap_path                 : pointer to snap path
* return                    : last node in path
*/
void *pop_node_from_snap_path(
    IN art_snap_path_t *snap_path);


/*
* description               : get last node info from snap path. 
* snap_path                 : pointer to snap path
* pos_in_parent             : node position in its parent'
* child_pos                 : current handling child pos
* parent_idx                : parent index in path
* return                    : last node if ok, else return NULL.
*/
void *get_last_node_in_snap_path(
    IN art_snap_path_t *snap_path,
    OUT uint32_t *pos_in_parent,
    OUT uint32_t *child_pos,
    OUT uint32_t *parent_idx);


/*
* description               : set last node current handling child pos. 
* snap_path                 : pointer to snap path
* child_pos                 : current handling child pos
*/
static inline void set_last_child_pos_in_snap_path(
    IO art_snap_path_t *snap_path,
    IN uint32_t child_pos)
{
    if(check_snap_path_empty(snap_path))
    {
        return;
    }

    snap_path->path[snap_path->path_index - 1].child_pos = child_pos;
}


/*
* description               : get last node current handling child pos. 
* snap_path                 : pointer to snap path
* return                    : current handling child pos
*/
static inline uint32_t get_last_node_child_pos_in_snap_path(
    IN art_snap_path_t *snap_path)
{
    if(check_snap_path_empty(snap_path))
    {
        return ART_INVALID_CHILD_POS;
    }
    
    return snap_path->path[snap_path->path_index - 1].child_pos;
}


/*
* description               : get snapid depth in path. 
* snap_path                 : pointer to snap path
*/
static inline uint32_t get_prefix_len_in_snap_path(
    IN art_snap_path_t *snap_path)
{
    return snap_path->prefix_len;
}


/*
* description               : clear prefix in snap path. 
* snap_path                 : pointer to snap path
*/
static inline void clear_snap_path_prefix(
    IN art_snap_path_t *snap_path)
{
    ART_MEMSET(snap_path->prefix, 0, snap_path->prefix_len);
    snap_path->prefix_len = 0;
}

void set_node_release_status_snap_path(
    IO art_snap_path_t *snap_path,
    IN bool is_need_release,
    IN void* buff_node);



void get_node_release_status_snap_path(
    IO art_snap_path_t *snap_path,
    OUT bool* is_need_release,
    OUT void** buff_node);



#ifdef __cplusplus
}
#endif
#endif


