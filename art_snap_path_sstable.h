#ifndef ART_SNAP_PATH_SSTABLE_H
#define ART_SNAP_PATH_SSTABLE_H

#include "art_iterator_opt.h"

#ifdef __cplusplus
extern "C" {
#endif


/**
 * get left/right most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_snap_path_get_lr_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f);

/**
 * get left most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_snap_path_get_left_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args);


/**
 * get right most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_snap_path_get_right_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args);


/**
 * switch path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @arg snap_get_next_child_f function pointer for get next/prev child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_snap_path_switch_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f,
    OUT int32_t *inner_result);


/**
 * switch prev path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_snap_path_switch_prev_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args);


/**
 * switch next path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_snap_path_switch_next_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args,
    OUT int32_t *inner_result);


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT
 */
int32_t sstable_snap_path_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args,
    OUT int32_t *inner_result);


/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT
 */
int32_t sstable_snap_path_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args);

void sstable_clear_snap_path(art_snap_path_t *snap_path);

#ifdef __cplusplus
}
#endif
#endif



