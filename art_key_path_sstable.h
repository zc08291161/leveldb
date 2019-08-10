#ifndef ART_KEY_PATH_SSTABLE_H
#define ART_KEY_PATH_SSTABLE_H

#include "art_iterator_opt.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * sstable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_key_path_get_lr_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC iterator_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC fusion_get_lr_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * sstable get key left most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_key_path_get_left_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * sstable get key right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_key_path_get_right_most_path(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * sstable switch path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @arg fusion_get_next_child_f function pointer for get next/prev in fusion child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_key_path_switch_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC fusion_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_NEXT_FUNC fusion_get_next_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result);


/**
 * sstable switch prev path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_key_path_switch_prev_path(
    IO art_key_path_t *key_path,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * sstable switch next path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_key_path_switch_next_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result);


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t sstable_key_path_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result);


/**
 * get a path that key equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return return kok means found, or other code.
 */
int32_t sstable_key_path_get_equal(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent);

void sstable_clear_key_path(art_key_path_t *key_path);

#ifdef __cplusplus
}
#endif
#endif



