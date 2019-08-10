#ifndef ART_SNAPSHOT_MEMTABLE_H_
#define ART_SNAPSHOT_MEMTABLE_H_

#include "art_memtable.h"
#include "art_snapshot.h"
//#include "art_pri.h"
#ifdef __cplusplus
extern "C" {
#endif

#pragma pack (1)

/**
 * Small art_snap node with only 4 children
 */
typedef struct
{
    art_snap_node                   n;
    uint8_t                         keys[4];
    memtable_art_child_t            children[4];
} memtable_art_snap_node4;


/**
 * art_snap Node with 16 children
 */
typedef struct
{
    art_snap_node                   n;
    uint8_t                         keys[16];
    memtable_art_child_t            children[16];
} memtable_art_snap_node16;


/**
 * art_snap Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct
{
    art_snap_node                   n;
    uint8_t                         keys[256];
    memtable_art_child_t            children[48];
} memtable_art_snap_node48;


/**
 * Full art_snap node with 256 children
 */
typedef struct
{
    art_snap_node                   n;
    memtable_art_child_t            children[256];
} memtable_art_snap_node256;

#pragma pack()

art_snap_node** memtable_snap_find_child(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN uint8_t c,
    OUT int* child_idx);

art_snap_node** add_snap_child4(
    IN art_tree_t* t,
    IN void* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args);

art_snap_node** add_snap_child16(
    IN art_tree_t* t,
    IN void* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args);

art_snap_node** add_snap_child48(
    IN art_tree_t* t,
    IN void* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args);

art_snap_node** add_snap_child256(
    IN void* node,
    IN uint8_t c,
    IN void* child);

void* memtable_get_snap_child_addr_by_index(art_snap_node* node, int idx);

void destroy_snap_node4(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback);

void destroy_snap_node16(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback);

void destroy_snap_node48(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback);

void destroy_snap_node256(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback);

art_snap_node** add_snap_child4_in_new_node(
    IN art_node* snap_node4,
    IN uint8_t c,
    IN void* child);

uint32_t get_memtable_snap_node_type_size(uint8_t node_type);

//uint8_t memtable_snap_node_get_key(art_node* node, int key_idx);

bool is_memtable_snap_inner_node(IN art_snap_node* node);

/**
 * get left child of the node
 * @art parent_node parent node
 * @arg child the child
 * @arg idx_in_parent the child's index in parent
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_snap_get_node_left_child(
    IN art_snap_node*  parent_node,    
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get right child of the node
 * @art parent_node parent node
 * @arg child the child
 * @arg idx_in_parent the child's index in parent
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_snap_get_node_right_child(
    IN art_snap_node* parent_node,
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent);


/**
 * get right child of the node
 * @art parent_node parent node
 * @arg start_idx prev child index
 * @arg child the child
 * @arg idx_in_parent the next child's index in parent
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_snap_get_node_next_child(
    IN art_snap_node* parent_node,
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent);


/**
 * get right child of the node
 * @art parent_node parent node
 * @arg start_idx prev child index
 * @arg child the child
 * @arg idx_in_parent the prev child's index in parent
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_snap_get_node_prev_child(
    IN art_snap_node* parent_node,
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent);


/**
 * get child node equal or greator than input key
 * @arg parent_node parent node
 * @arg key the key
 * @arg key_len the key length
 * @arg depth valid key start index in key
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return ART_COMPARE_RESULT.
 */
int32_t memtable_snap_search_node_equal_or_bigger(
    IN  art_snap_node*  parent_node,
    IN  uint32_t snapid,
	IN  uint32_t snapid_len,
	IN  uint32_t depth,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get most right child of the snap root node
 * @arg snap_root leaf node or snap root node
 * @arg is_snap_root_leaf is snap root leaf node
 * @return leaf.
 */
void* memtable_snap_get_most_right(
    IN art_snap_node* snap_root,
    OUT bool *is_snap_root_leaf);
/*
* description: batch search one key in memtable snap tree.
*/
int32_t memtable_snap_search_node_equal(
	art_snap_node*  parent_node,
	uint32_t snapid,
	uint32_t snapid_len,
	uint32_t depth,
	art_snap_node** child,
	uint32_t*  idx_in_parent);

#ifdef __cplusplus
}
#endif
#endif

