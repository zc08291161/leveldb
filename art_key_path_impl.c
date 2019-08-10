#include "art_key_path_impl.h"


#ifdef __cplusplus
extern "C" {
#endif


/*
* description               : init key path.
* key_path                  : pointer to key path
*/
void init_key_path(
    IO art_key_path_t *key_path)
{
    ART_MEMSET(key_path, 0, sizeof(art_key_path_t));
}


/*
* description               : init key path operations.
* key_path                  : pointer to key path
* ops                       : operation set
*/
void init_key_path_ops(
    IO art_key_path_t *key_path,
    IN art_path_operations_t *ops)
{
    key_path->ops.read_plog_f = ops->read_plog_f;
    key_path->ops.direct_read_f = ops->direct_read_f;
    key_path->ops.get_buffer_f = ops->get_buffer_f;
    key_path->ops.release_buffer_f = ops->release_buffer_f;
}


/*
* description               : assign internal operation set for key path
* key_path                  : pointer to key path
* ops                       : internal operation set
*/
void assign_key_path_internal_ops(
    IO art_key_path_t *key_path,
    IN artree_operations_t *ops)
{
    key_path->art_ops = ops;
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
    IN uint32_t pos_in_parent)
{
    uint32_t parent_idx         = 0;
    uint32_t index              = key_path->path_index;

    if(0 < index)
    {
        parent_idx = index - 1;
    }
    key_path->path[index].node = node;
    key_path->path[index].pos_in_parent = pos_in_parent;
    key_path->path[index].child_pos = ART_INVALID_CHILD_POS;
    key_path->path[index].parent_idx = parent_idx;
    key_path->path_index++;
}

/*
* description               : pop last node from snap path.
* key_path                  : pointer to key path
* return                    : last node in path
*/
void *pop_node_from_key_path(
    IN art_key_path_t *key_path)
{
    void *node                  = NULL;
    uint32_t index              = key_path->path_index - 1;

    if(check_key_path_empty(key_path))
    {
        return NULL;
    }

    node = key_path->path[index].node;
    //ART_MEMSET(&key_path->path[index], 0, sizeof(art_path_node_t));
    key_path->path_index--;

    return node;
}

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
    OUT uint32_t *parent_idx)
{
    uint32_t index                  = 0;
    
    if(check_key_path_empty(key_path))
    {
        return NULL;
    }

    index = key_path->path_index - 1;
    *pos_in_parent = key_path->path[index].pos_in_parent;
    *child_pos = key_path->path[index].child_pos;
    *parent_idx = key_path->path[index].parent_idx;

    key_path->is_need_release = key_path->path[index].is_need_release;
    key_path->buff_node = key_path->path[index].buff_node;

    return key_path->path[index].node;
}

void set_node_release_status_key_path(
    IO art_key_path_t *key_path,
    IN bool is_need_release,
    IN void* buff_node)
{
    key_path->path[key_path->path_index].is_need_release = is_need_release;
    key_path->path[key_path->path_index].buff_node = buff_node;
    //key_path->is_need_release = TRUE;
    //key_path->buff_node = NULL;
}


void get_node_release_status_key_path(
    IO art_key_path_t *key_path,
    OUT bool* is_need_release,
    OUT void** buff_node)
{
   *is_need_release = key_path->path[key_path->path_index].is_need_release;
   *buff_node = key_path->path[key_path->path_index].buff_node;
}


#ifdef __cplusplus
}
#endif

