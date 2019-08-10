#ifndef ART_MEMTABLE_IO_H_
#define ART_MEMTABLE_IO_H_

#include "art_pri.h"
#include "art_path.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : batch search in memtable.
* get_path                  : batch get struct, contains artree and path
* parent_node               : root node
* para                      : the param of batch get
* get_callback              : return value to upper
* return                    : kOk or kNotFound
*/
int32_t memtable_batch_get(
            batch_get_path_t *get_path, 
            art_node *parent_node, 
            artree_batch_get_para_t *para, 
            ARTREE_GET_CALLBACK get_callback);

/**
 * make a new snap node4 and a leaf,then add leaf to node4
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg prefix_diff The depth of insert
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert successfully, otherwise
 * means failed.
 */
int32_t add_new_leaf_2_new_snap_node(
    IN art_tree_t* t,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN int prefix_diff,
    OUT art_node** new_node4,
    OUT void** new_leaf,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls);

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg snapid snapshot Id
 * @arg type The type of value,see ValueType
 * @arg value Opaque value.
 * @arg value_len The length of the value.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert seccessfully, otherwise
 * insert failed.
 */
int32_t art_insert(
    IN art_tree_t* t,
    IN const uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapid,
    IN uint8_t type,
    IN const uint8_t* value,
    IN uint32_t value_len,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args);

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg insert_para the para
 * @return kOk means insert seccessfully, otherwise
 * insert failed.
 */
int32_t batch_insert(
    IN art_tree_t* t,
    IN artree_batch_insert_para_t *batch_insert_para);

void** insert_2_end_child(
    IN art_tree_t* t,
    IN art_node** node,
    IN art_insert_para_t* insert_para,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args);

/**
 * make a leaf node
 * @arg t The tree
 * @arg insert_para The parameters of operation
 * @arg snapid_len The length of snapid,not used.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg pcls The function arg.
 * @return NULL means alloc failed, otherwise
 * means successfully.
 */
void* make_snap_leaf(    
    IN art_tree_t* t,
    IN art_insert_para_t* insert_para,
    IN int snapid_len,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls);

int32_t memtable_snap_seek_get_path(
	IN art_snap_iterator* iterator,
	IN art_snap_node* snap_node,
	IN artree_seek_para_t *seek_para);

int32_t memtable_snap_seek_update_path(
	IN art_snap_iterator* iterator,
	IN artree_seek_para_t *seek_para);

/*
*   use iterator to complete seek()
*   1.use art_iterator_seek_snap to get the path
*   2.use art_iterator_prev to get the
*
*/
int32_t memtable_snap_seek(
    IN art_tree_t* t,
    IN art_snap_node* snap_node,
    IN artree_seek_para_t *seek_para);

/*
uint8_t memtable_get_key(
    IN art_node* node,
    IN int key_idx);
*/

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
int32_t memtable_io_search_key(
    IN art_tree_t* t,
    IN art_node *parent_node,
    IN const uint8_t* key,
    IN const uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    OUT art_snap_node** snap_node);


#if DESC("memtable get max&min key interface")
int32_t memtable_io_get_min_or_max_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
	IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len);


int32_t memtable_io_get_min_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len);


int32_t memtable_io_get_max_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len);
#endif

#ifdef __cplusplus
}
#endif

#endif
