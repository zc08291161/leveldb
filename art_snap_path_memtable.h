#ifndef ART_SNAP_PATH_MEMTABLE_H
#define ART_SNAP_PATH_MEMTABLE_H

#ifdef __cplusplus
extern "C" {
#endif


/**
 * get left/right most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_snap_path_get_lr_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f);


/**
 * get left most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_snap_path_get_left_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent);


/**
 * get right most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @return kOk means ok, otherwise something wrong.
int32_t memtable_snap_path_get_right_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent); */


/**
 * switch path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @arg snap_get_next_child_f function pointer for get next/prev child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_snap_path_switch_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f);


/**
 * switch prev path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.

int32_t memtable_snap_path_switch_prev_path(
    IO art_snap_path_t *snap_path); */


/**
 * switch next path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_snap_path_switch_next_path(
    IO art_snap_path_t *snap_path);


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT
 */
int32_t memtable_snap_path_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid);


/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT
 */
int32_t memtable_snap_path_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid);


void memtable_clear_snap_path(art_snap_path_t *snap_path);


#ifdef __cplusplus
}
#endif
#endif


