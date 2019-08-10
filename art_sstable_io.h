#ifndef ART_SSTABLE_IO_H_
#define ART_SSTABLE_IO_H_

#include "art_pri.h"
#include "art_fkey_sstable.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : batch search in sstable.
* get_path                  : batch get struct, contains artree and path
* node                      : the first node of sstable search
* get_callback              : return value to upper
*/
int32_t sstable_batch_get(
            art_tree_t *artree, 
            art_node *node, 
            artree_batch_get_para_t *para, 
            ARTREE_GET_CALLBACK get_callback,
            BATCH_READ_PLOG_FUNC batch_read_plog_callback);

/**
* Searches for a value in the ART tree
* @arg t The tree
* @arg key The key
* @arg key_len The length of the key
* @return NULL if the item was not found, otherwise
* the value pointer is returned.
*/
int32_t art_sstable_search_io(
	IN art_tree_t* t,
	IN const uint8_t* key,
	IN uint32_t key_len,
	IN void *args,
	IN uint32_t snapshotid,
    IN uint32_t type,
	OUT art_data** data,
	OUT bool* is_fusion_node_leaf,
    OUT void** fusion_origin_node,
    OUT uint32_t* value_len,
    OUT art_snap_node** snap_node);


 /**
 * search value from the snap node
 * @arg t The tree
 * @arg key The node
 * @arg snapId the snapid to be found
 * @arg value the return value
 * @arg value_len  length of value
 * @return kOK means successfully.
 */
int32_t sstable_snap_search(
    IN art_tree_t* t,
    IN void* node,
    IN artree_get_para_t *get_para);


/**
 * get a max key from the node
 * @arg t The tree
 * @arg key The node
 * @arg key_buff_len The length of the key
 * @arg read_plog_callback
 * @arg args
 * @arg key the return key
 * @arg index The index
 * @return kOK means successfully.
 */
int32_t sstable_io_get_max_key(
    IN art_tree_t* t,
    IN void* node,
    IN uint32_t key_buff_len,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args,
    OUT uint8_t* key,
    OUT uint32_t* index);


 /**
 * get a min key from the node
 * @arg t The tree
 * @arg key The node
 * @arg key_buff_len The length of the key
 * @arg read_plog_callback
 * @arg args
 * @arg key the return key
 * @arg index The index
 * @return kOK means successfully.
 */
int32_t sstable_min_key_io(
    IN art_tree_t* t,
    IN void* node,
    IN uint32_t key_buff_len,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args,
    OUT uint8_t* key,
    OUT uint32_t* index);

int32_t snap_seek_get_valid_leaf(
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para);

void snap_seek_release_path(
    IN RELEASE_BUFFER_FUNC release_buffer,
    IN art_snap_iterator* iterator);

int32_t sstable_snap_seek_get_path(
	IN art_snap_iterator* iterator,
	IN void* origin_node,
	IN artree_seek_para_t *seek_para);

int32_t sstable_snap_seek_update_path(
	IN art_snap_iterator* iterator,
	IN artree_seek_para_t *seek_para);

/*
*   use iterator to complete seek()
*   1.use art_iterator_seek_snap to get the path
*   2.use art_iterator_prev to get the
*
*/
int32_t sstable_snap_seek(
    IN art_tree_t* t,
    IN void* node,
    IN artree_seek_para_t *seek_para);

#ifdef __cplusplus
}
#endif

#endif