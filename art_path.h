#ifndef ART_PATH_H
#define ART_PATH_H

#include "art_define.h"
#include "art_pub.h"
#include "art.h"
#include "art_pri.h"

#ifdef __cplusplus
extern "C" {
#endif


// memtable art node get left or right child
typedef int32_t             (*MEMTABLE_GET_NODE_LR_CHILD_FUNC)(art_node *parent_node, art_node **child, uint32_t *idx_in_parent); // memtable get left/right child func
// memtable art snap node get left or right child
typedef int32_t             (*MEMTABLE_GET_NODE_NEXT_FUNC)(art_node* parent_node, uint32_t start_idx, art_node** child, uint32_t* idx_in_parent); // memtable get next/prev child func
// memtable art node get next or prev child
typedef int32_t             (*MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC)(art_snap_node *parent_node, art_snap_node **child, uint32_t *idx_in_parent); // memtable snap get left/right child func
// memtable art snap node get next or prev child
typedef int32_t             (*MEMTABLE_SNAP_GET_NODE_NEXT_FUNC)(art_snap_node* parent_node, uint32_t start_idx, art_snap_node** child, uint32_t* idx_in_parent); // memtable snap get next/prev child func


// sstable art node get left or right child
typedef int32_t             (*SSTABLE_GET_NODE_LR_CHILD_FUNC)(art_node* parent_node, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);
// sstable art snap node get left or right child
typedef int32_t             (*SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC)(art_snap_node* parent_node, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);
// sstable art node get next or prev child
typedef int32_t             (*SSTABLE_GET_NODE_NEXT_FUNC)(art_node* parent_node, uint32_t start_idx, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);
// sstable art snap node get next or prev child
typedef int32_t             (*SSTABLE_SNAP_GET_NODE_NEXT_FUNC)(art_snap_node* parent_node, uint32_t start_idx, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);

// sstable fusion node get left or right child
typedef int32_t             (*SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC)(art_node* parent_node, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);
// sstable fusion node get next or prev child
typedef int32_t             (*SSTABLE_GET_FUSION_NODE_NEXT_FUNC)(art_node* parent_node, uint32_t start_idx, READ_PLOG_FUNC read_plog_callback, void *args, void** child, uint32_t* idx_in_parent);


/**
* use to record the path of art in flush function.
*/
typedef struct
{
	void* node;
	void* origin_node;

	int pos_in_parentNode;
	/*node4 16 256存的是children的下标,48存的是key的下标，
	通过keys[idx] - 1获取children下标:update path的时候，从key idx处递增查询*/

	int parent_node_idx;
	int depth;
} flush_path_t;


typedef struct
{
    READ_PLOG_FUNC                  read_plog_f;
    DIRECT_READ_PLOG_FUNC           direct_read_f;
    GET_BUFFER_FUNC                 get_buffer_f;
    RELEASE_BUFFER_FUNC             release_buffer_f;
}art_path_operations_t;


typedef struct
{
    void                            *node; // node in tree path
    uint32_t                        pos_in_parent; // its position in parent node
    uint32_t                        child_pos; // current handling child pos
    uint32_t                        parent_idx; // parent index in path
    bool                            is_need_release; //for sstable, false means read from itertor buff
    void*                           buff_node; //iterator buff node manager
}art_path_node_t;


// key path
typedef struct
{
    art_path_node_t                 path[MAX_KEY_LEN]; // not include snap inner node and leaf
    uint32_t                        path_index; // point to the path after last node
    
    uint8_t                         prefix[MAX_KEY_LEN];
    uint32_t                        prefix_len;

    art_path_operations_t           ops;
    artree_operations_t             *art_ops; // internal operation set

    void*                           iterator;
    bool                            is_need_release; //node status which is not in path
    void*                           buff_node; 
}art_key_path_t;


// snap path
typedef struct
{
    art_path_node_t                 path[SNAPID_LEN + 1]; // not include snap inner node and leaf
    uint32_t                        path_index; // point to the path after last node
    
    uint8_t                         prefix[SNAPID_LEN + 1]; // include leaf
    uint32_t                        prefix_len;

    art_path_operations_t           ops;
    artree_operations_t             *art_ops; // internal operation set

    void*                           iterator;
    bool                            is_need_release; //node status which is not in path
    void*                           buff_node; 
}art_snap_path_t;


typedef struct
{
    art_key_path_t                  key_path;
    art_snap_path_t                 snap_path;
    art_tree_t                      *t;
}art_path_t;

/* path for batch get */
typedef struct
{
	art_tree_t                  *artree;
	art_path_t                  art_path;
}batch_get_path_t;

typedef enum
{
    //与其他内部错误码错开，以免出现误判
    CHECK_OK = 10,
    KEY_LEN_INVALID,
    KEY_INVALID,
    CHECK_KEY_OUT_OF_RANGE,
    CHECK_SNAPSHOTID_OUT_OF_RANGE,
    CHECK_STATUS_BUFF
}CHECK_STATUS_E;

/*
* description               : init path structure.
* key_path                  : pointer to path
* t                         : pointer to artree
* read_args                 : read plog para
*/
void init_art_path(
    IN art_path_t *path,
    IN art_tree_t *t);


/*
* description               : init art path operations.
* path                      : pointer to art path
* ops                       : operation set
*/
void init_art_path_ops(
    IO art_path_t *path,
    IN art_path_operations_t *ops);


/*
* description               : assign internal operation set for art path
* path                      : pointer to art path
* ops                       : internal operation set
*/
void assign_art_path_internal_ops(
    IO art_path_t *path,
    IN artree_operations_t *ops);


/*
* description               : clear key and snap path. 
* art_path                  : pointer to path
*/
void clear_art_path(
	IN art_path_t *art_path,
	IN const char* func,
	IN int32_t line);


/*
* description               : get min path. 
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_first(
    IN art_path_t *art_path,
    IN void *args);


/*
* description               : get max path. 
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_last(
    IN art_path_t *art_path,
    IN void *args);


/*
* description               : get prev path. 
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_prev(
    IN art_path_t *art_path,
    IN void *args);


/*
* description               : get next path. 
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_next(
    IN art_path_t *art_path,
    IN void *args);

int32_t art_path_get_next_with_condition(
    IN art_tree_t* t,
    IN art_path_t *art_path,
    IN void *args,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN bool return_snap_once);

/**
 * get a path that key bigger or equal than input.key
 * @arg path pointer to path
 * @arg key
 * @arg key_len
 * @arg snapshotid
 * args read plog para
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t art_path_get_bigger_or_equal(
    IN art_path_t *art_path,
    IN uint8_t *key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args);

int32_t art_path_get_bigger_or_equal_with_condition(
    IN art_path_t *art_path,
    IN art_tree_t* t,
    IN uint8_t *starting_key,
    IN uint32_t starting_key_len,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN void *args);

/**
 * get a path that key equal than input.key, and with smallest snap id
 * @arg path pointer to path
 * @arg key
 * @arg key_len
 * @arg read plog para
 * @return return kok means found, or other code.
 */
int32_t art_path_get_equal_key_smallest_snapid(
    IO art_path_t *art_path,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args);


/**
 * get key from path
 * @arg path pointer to path
 * @out key
 * @out key_len
 * @return return kok means found, or other code.
 */
int32_t art_path_get_key(
    IO art_path_t *art_path,
    IN uint8_t** key,
    IN uint32_t* key_len);


/**
 * get value from path
 * @arg path pointer to path
 * @out value
 * @out value_len
 * @return return kok means found, or other code.
 */
int32_t art_path_get_value(
    IO art_path_t *art_path,
    OUT uint8_t** value,
    OUT uint32_t* value_len);


/**
 * get snapid from path
 * @arg path pointer to path
 * @out snapid
 * @return return kok means found, or other code.
 */
int32_t art_path_get_snapid(
    IO art_path_t *art_path,
    OUT uint32_t* snapid);


/**
 * check whether the value of op code is to delete
 * @arg path pointer to path
 * @out isdelete
 * @return return kok means found, or other code.
 */
int32_t art_path_check_is_delete(
    IO art_path_t *art_path,
    OUT bool* isdelete);


/**
 * check whether check if the path is valid. 
 * @arg path pointer to path
 * @out isvalid
 * @return return kok means found, or other code.
 */
int32_t art_path_check_is_valid(
    IO art_path_t *art_path,
    OUT bool* isvalid);

void* reuse_node_impl(batch_get_path_t *get_path, art_node *node, uint8_t *key, uint32_t key_len, uint8_t *snapid);

/*
* description               : clear batch get path.
* get_path                  : batch get struct, contains artree and path
*/
void art_clear_path(batch_get_path_t *get_path);

void get_value_callback(art_tree_t *artree, art_path_t *art_path, void *args, void *instance, uint32_t index, ARTREE_GET_CALLBACK get_callback);

int32_t check_condition_key_valid(uint8_t *prefix_key, uint32_t prefix_key_len, uint8_t *key, uint32_t key_len);

int32_t check_condition_snapid_valid(uint32_t snapid_min, uint32_t snapid_max, uint32_t snapid);

#ifdef __cplusplus
}
#endif
#endif


