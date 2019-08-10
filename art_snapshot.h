#ifndef ART_SNAPSHOT_H_
#define ART_SNAPSHOT_H_
#include "art_define.h"
#include "art_pri.h"
#ifndef _SCM_PCLINT_
#include "art_leaf.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif
  
typedef enum
{
    PathEqualToSnapId         = 0,
    PathGreaterToSnapId       = 1,
    PathLessToSnapId          = 2
} SNAP_COMPARE_CODE_TYPE;

/**
 * use to record the path of art in flush function.
 */
typedef struct
{
    void* node;
    int pos_in_parentNode; 
    /*node4 16 256存的是children的下标,48存的是key的下标，
    通过keys[idx] - 1获取children下标:update path的时候，从key idx处递增查询*/
    
    SNAP_COMPARE_CODE_TYPE flag;
} snap_path_t;

typedef struct 
{
    snap_path_t path[SNAPID_LEN+1];//include leaf
    uint32_t path_index;
    
    uint32_t snapid_length;

    art_tree_t* t;
}art_snap_iterator;

bool snap_leaf_matches(IN art_tree_t* t, IN void* leaf, IN uint32_t snapid);

int snap_check_prefix(
    IN const art_snap_node* n,
    IN const uint32_t snapid,
    IN int snapid_len,
    IN int depth);

void snap_copy_partial(
    IN art_snap_node* new_node,
    IN const uint32_t snapid,
    IN uint32_t depth,
    IN uint32_t longest_prefix);

int32_t snap_seek_get_path_node256_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_get_path_node48_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_get_path_node16_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_get_path_node4_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_update_node256(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_update_node48(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_update_node16(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para);

int32_t snap_seek_update_node4(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para);

bool check_art_snap_node256_valid(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN uint8_t child_index);

uint8_t snap_get_key_by_idx(
    IN art_snap_node* node,
    IN int idx);

bool is_snap_inner_node(IN art_snap_node* node);

#ifdef __cplusplus
}
#endif

#endif
