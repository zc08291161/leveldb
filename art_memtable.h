#ifndef ART_MEMTABLE_H_
#define ART_MEMTABLE_H_

#include "art_memtable_pri.h"
#include "art_pri.h"
#ifndef _SCM_PCLINT_
#include "art_leaf.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif
#pragma pack (8)

/**
 * Small art node with only 4 children
 */
typedef struct
{
    art_node                        n;
    uint8_t                         keys[4];
    memtable_art_child_t            end; /*特殊指针，支持变长*/
    memtable_art_child_t            children[4];
    uint32_t                        max_snapid; /*max snapshot id of all children*/
} memtable_art_node4;


/**
 * art Node with 16 children
 */
typedef struct
{
    art_node                        n;
    uint8_t                         keys[16];
    memtable_art_child_t            end;/*特殊指针，支持变长*/
    memtable_art_child_t            children[16];
    uint32_t                        max_snapid; /*max snapshot id of all children*/
} memtable_art_node16;


/**
 * art Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct
{
    art_node                        n;
    uint8_t                         keys[256];
    memtable_art_child_t            end;/*特殊指针，支持变长*/
    memtable_art_child_t            children[48];
    uint32_t                        max_snapid; /*max snapshot id of all children*/
} memtable_art_node48;


/**
 * Full art node with 256 children
 */
typedef struct
{
    art_node                        n;
    memtable_art_child_t            end;/*特殊指针，支持变长*/
    memtable_art_child_t            children[256];
    uint32_t                        max_snapid; /*max snapshot id of all children*/
} memtable_art_node256;

#pragma pack()


 /**
 * get current node's end child
 * @arg node The current node
 * @return NULL means end child is NULL.
 */
void** memtale_get_end_child(IN art_node* node);

void memtable_new_node_set_end_valid(art_node* memtable_node4, uint8_t flag);

void memtable_new_node_set_partial(art_node* memtable_node4, uint8_t* tmp_node, uint32_t prefix_diff);

void destroy_node(
    IN void* node,
    IN FREE_MEM_FUNC arena_destroy_callback);

art_node** add_child_in_new_node(
    IN void* parent_node,
    IN uint8_t c,
    IN void* child);

/**
 * get most right child of the snap root node
 * @arg snap_root leaf node or snap root node
 * @arg is_snap_root_leaf is snap root leaf node
 * @return leaf.
 */
void* memtable_snap_get_most_right(
    IN art_snap_node* snap_root,
    OUT bool *is_snap_root_leaf);

void copy_node_buff(IN art_node* dest, IN art_node* src);

art_node** add_child(
	IN art_tree_t* t,
	IN void* node,
	IN void** ref,
	IN uint8_t c,
	IN void* child,
	IN ALLOC_MEM_FUNC arena_alloc_callback,
	IN void* args);

#if DESC("memtable inner node interface")
static inline bool is_memtable_inner_node(IN art_node* node)
{
    return ((node->node_type >= MEMTABLE_ART_NODE4) && (node->node_type <= MEMTABLE_ART_NODE256));
}

uint32_t get_memtable_node_type_size(uint8_t node_type);

/*Copy the header info of the node*/
static inline void memtable_node_copy_header(IN art_node* dest, IN art_node* src)
{
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    dest->end_valid = src->end_valid;
    ART_MEMCPY(dest->partial, src->partial, src->partial_len);
}


uint8_t memtable_node_get_key(IN art_node* node, IN int key_idx);


void memtable_node_set_child(
    IN art_node* node,
    IN int32_t idx,
    IN void* child);


uint8_t memtable_node_get_key_by_idx(
    IN art_node* memtable_node, 
    IN int idx);


void* memtable_node_get_child_by_child_idx(
    IN art_node* node, 
    IN int idx);


void memtable_node_set_end_child(
    IN art_node* node, 
    IN void* child);


/**
 * find a child by key
 * @arg node The parent node
 * @arg c A byte of the key
 * @arg child_idx The index of child
 * @return the child.
 */
art_node** memtable_node_find_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT int* child_idx);


/**
 * get left child of the node
 * @art parent_node parent node
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_get_node_left_child(
    IN  art_node*  parent_node,    
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get right child of the node
 * @arg parent_node parent node
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_get_node_right_child(
    IN  art_node*  parent_node,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get next child of the node
 * @arg parent_node parent node
 * @arg start_idx the current child index
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_get_node_next_child(
    IN  art_node*  parent_node,
    IN  uint32_t   start_idx,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get prev child of the node
 * @arg parent_node parent node
 * @arg start_idx the current child index
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_get_node_prev_child(
    IN  art_node*  parent_node,
    IN  uint32_t   start_idx,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get child node equal or greator than input key
 * @arg parent_node parent node
 * @arg key the key
 * @arg key_len the key length
 * @arg depth valid key start index in key
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return ART_COMPARE_RESULT
 */
int32_t memtable_search_node_equal_or_bigger(
    IN  art_node*  parent_node,
    IN  uint8_t* key,
	IN  uint32_t key_len,
	IN  uint32_t depth,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);


/**
 * get child node equal with input key
 * @arg parent_node parent node
 * @arg key the key
 * @arg key_len the key length
 * @arg depth valid key start index in key
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_search_node_equal(
    IN  art_node*  parent_node,
    IN  uint8_t* key,
	IN  uint32_t key_len,
	IN  uint32_t depth,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent);

#endif




#if DESC("memtable snap inner node interface")
/**
 * get child node equal or smaller than input key
 * @arg parent_node parent node
 * @arg key the key
 * @arg key_len the key length
 * @arg depth valid key start index in key
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return ART_COMPARE_RESULT.
 */
int32_t memtable_snap_search_node_equal_or_smaller(
    IN  art_snap_node*  parent_node,
    IN  uint32_t snapid,
	IN  uint32_t snapid_len,
	IN  uint32_t depth,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent);
#endif

#ifdef __cplusplus
}
#endif
#endif

