#ifndef ART_PATH_MEMTABLE_H
#define ART_PATH_MEMTABLE_H

#include "art_define.h"
#include "art_path.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * memtable get left/right most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @out snap_get_lr_child_f function pointer for get left/right in snap child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_lr_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f);


/**
 * memtable get left most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_left_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent);


/**
 * memtable get right most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_right_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent);


/**
 * memtable switch path entrance
 * @arg path, pointer to path
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @arg snap_get_lr_child_f function pointer for get left/right in snap child
 * @arg snap_get_next_child_f function pointer for get next/prev in snap child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_path(
    IO art_path_t *path,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f);


/**
 * memtable switch prev path entrance
 * @arg path, pointer to path
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_prev_path(
    IO art_path_t *path);


/**
 * memtable switch next path entrance
 * @arg path, pointer to path
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_next_path(
    IO art_path_t *path);

int32_t memtable_path_switch_next_path_with_condition(
    IN art_tree_t* t,
    IO art_path_t *path,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN bool return_snap_once);

/**
 * get a path that key bigger or equal than input.key
 * @arg path pointer to path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t memtable_path_get_bigger_or_equal(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t);
	
int32_t memtable_path_get_bigger_or_equal_with_condition(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t *starting_key,
    IN uint32_t starting_key_len,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN art_tree_t* t);

/**
 * get a path that key equal than input.key, and with smallest snap id
 * @arg path pointer to path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para 
 * @return return kok means found, or other code.
 */
int32_t memtable_path_get_equal_key_smallest_snapid(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len);


/**
 * get value from path
 * @arg art_path path, last node in path is fusion node
 * @arg value. output value
 * @arg value_len. output value length
 * @return kok means found, or other code.
 */
int32_t memtable_path_get_value(
    IN art_path_t *path,
    OUT uint8_t **value, 
    OUT uint32_t *value_len);


/**
 * get snapid from path
 * @arg art_path path, last node in path is fusion node
 * @arg snap_id. output snapshot id
 * @return kok means found, or other code.
 */
int32_t memtable_path_get_snapid(
    IN art_path_t *path,
    OUT uint32_t *snap_id);


/**
 * check whether the value of op code is to delete
 * @arg art_path path, last node in path is fusion node
 * @arg isdeleted. output check op code is delete
 * @return kok means found, or other code.
 */
int32_t memtable_path_check_is_delete(
    IN art_path_t *path, 
    OUT bool *isdeleted);

#ifdef __cplusplus
}
#endif
#endif




