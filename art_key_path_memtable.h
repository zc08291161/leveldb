#ifndef ART_KEY_PATH_MEMTABLE_H
#define ART_KEY_PATH_MEMTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * memtable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_key_path_get_lr_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * memtable get key left most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_key_path_get_left_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * memtable get key right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_key_path_get_right_most_path(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * memtable switch next/prev path at last node in path
 * @arg key_path, pointer to key path
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_key_path_switch_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * memtable switch prev path at last node in path
 * @arg key_path, pointer to key path
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_key_path_switch_prev_path(
    IO art_key_path_t *key_path,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * memtable switch next path at last node in path
 * @arg key_path, pointer to key path
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_key_path_switch_next_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t memtable_key_path_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);


/**
 * get a path that key equal than input.key
 * @arg key_path pointer to key path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return kok means found, or other code.
 */
int32_t memtable_key_path_get_equal(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent);

void memtable_clear_key_path(art_key_path_t *key_path);

#ifdef __cplusplus
}
#endif
#endif


