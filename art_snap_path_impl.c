#include "art_snap_path_impl.h"


#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : init snap path.
* snap_path                 : pointer to snap path
*/
void init_snap_path(
    IO art_snap_path_t *snap_path)
{
    ART_MEMSET(snap_path, 0, sizeof(art_snap_path_t));
}


/*
* description               : init snap path operations.
* snap_path                 : pointer to snap path
* ops                       : operation set
*/
void init_snap_path_ops(
    IO art_snap_path_t *snap_path,
    IN art_path_operations_t *ops)
{
    snap_path->ops.read_plog_f = ops->read_plog_f;
    snap_path->ops.direct_read_f = ops->direct_read_f;
    snap_path->ops.get_buffer_f = ops->get_buffer_f;
    snap_path->ops.release_buffer_f = ops->release_buffer_f;
}


/*
* description               : assign internal operation set for key path
* snap_path                 : pointer to snap path
* ops                       : internal operation set
*/
void assign_snap_path_internal_ops(
    IO art_snap_path_t *snap_path,
    IN artree_operations_t *ops)
{
    snap_path->art_ops = ops;
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
    IN uint32_t pos_in_parent)
{
    uint32_t parent_idx         = 0;
    uint32_t index              = snap_path->path_index;

    if(0 < index)
    {
        parent_idx = index - 1;
    }
    snap_path->path[index].node = node;
    snap_path->path[index].pos_in_parent = pos_in_parent;
    snap_path->path[index].child_pos = ART_INVALID_CHILD_POS;
    snap_path->path[index].parent_idx = parent_idx;
    snap_path->path_index++;
}

/*
* description               : pop last node from path.
* snap_path                 : pointer to snap path
* return                    : last node in path
*/
void *pop_node_from_snap_path(
    IN art_snap_path_t *snap_path)
{
    void *node                  = NULL;
    uint32_t index              = snap_path->path_index - 1;

    if(check_snap_path_empty(snap_path))
    {
        return NULL;
    }

    node = snap_path->path[index].node;
    //ART_MEMSET(&snap_path->path[index], 0, sizeof(art_path_node_t));
    snap_path->path_index--;

    return node;
}


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
    OUT uint32_t *parent_idx)
{
    uint32_t index                  = 0;
    
    if(check_snap_path_empty(snap_path))
    {
        return NULL;
    }

    index = snap_path->path_index - 1;
    *pos_in_parent = snap_path->path[index].pos_in_parent;
    *child_pos = snap_path->path[index].child_pos;
    *parent_idx = snap_path->path[index].parent_idx;

    snap_path->is_need_release = snap_path->path[index].is_need_release;
    snap_path->buff_node = snap_path->path[index].buff_node;

    return snap_path->path[index].node;
}


void set_node_release_status_snap_path(
    IO art_snap_path_t *snap_path,
    IN bool is_need_release,
    IN void* buff_node)
{
    snap_path->path[snap_path->path_index].is_need_release = is_need_release;
    snap_path->path[snap_path->path_index].buff_node = buff_node;
}


void get_node_release_status_snap_path(
    IO art_snap_path_t *snap_path,
    OUT bool* is_need_release,
    OUT void** buff_node)
{
   *is_need_release = snap_path->path[snap_path->path_index].is_need_release;
   *buff_node = snap_path->path[snap_path->path_index].buff_node;
}


#ifdef __cplusplus
}
#endif

