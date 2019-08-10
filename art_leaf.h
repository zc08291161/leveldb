#ifndef ART_LEAF_H_
#define ART_LEAF_H_

#include "art_define.h"
#include "art_node_type.h"
#include "art_pub.h"
#ifdef __cplusplus
extern "C" {
#endif
#pragma pack (1)

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 * use value[0] to support various value_len.
 * shared by memtable and sstable.
 */
typedef struct
{
    uint8_t                 node_type; /*必须放在头部*/
    
#ifdef ARTREE_DEBUG
    uint8_t                 check_rr_flag;// use for check read() and release() flags
#endif

    uint8_t                 op_type;
    uint32_t                snapid;
    uint8_t                 key_len;
    uint32_t                value_len; // support different size
    uint8_t                 key_value[0];
} art_leaf;


/**
 * Represents a leaf with small length of value. value len will be got from tree cfg.
 * shared by memtable and sstable
 * for fuison
 */
typedef struct
{
    uint8_t                 node_type; /*必须放在头部*/
    
#ifdef ARTREE_DEBUG
    uint8_t                 check_rr_flag;// use for check read() and release() flags
#endif

    uint8_t                 op_type;
    uint8_t                 value_len; // support different size
    uint32_t                snapid;
    uint8_t                 key_value[0];
} art_svalue_leaf;

#pragma pack()


typedef struct
{
    uint8_t                     (*get_op_type)(void *leaf);
    uint32_t                    (*get_snap_id)(void *leaf);
    uint32_t                    (*get_value_len)(void *leaf);
    uint8_t*                    (*get_value)(void *leaf);

    void                        (*set_op_type)(void *leaf, uint8_t op_type);
    void                        (*set_snap_id)(void *leaf, uint32_t snapid);
    void                        (*set_value)(void *leaf, uint8_t *value, uint32_t value_len);

    uint32_t                    (*calc_leaf_size)(uint32_t value_len, uint8_t verify_key_len);
    bool                        (*check_key_valid)(void *leaf, uint8_t *key, uint32_t key_len);
    void                        (*set_leaf_key)(void *leaf, uint8_t *key, uint32_t key_len);
    uint8_t*                    (*get_leaf_key)(void* leaf);
    void                        (*set_leaf_key_len)(void *leaf,  uint32_t key_len);
    uint32_t                    (*get_leaf_key_len)(void* leaf);
}art_leaf_operations_t;




#define is_art_leaf_node(node) ((*(uint8_t*)node) == MEMTABLE_ART_LEAF)


#if DESC("variable length leaf ops")
/*calc leaf of variable value size*/
static inline uint32_t calc_leaf_size(IN uint32_t value_len, IN uint8_t verify_key_len)
{
    return sizeof(art_leaf) + sizeof(uint8_t) * (uint32_t)(verify_key_len + value_len);
}


/*get the snapid of leaf*/
static inline uint32_t get_leaf_snapid(void* leaf)
{
    return ((art_leaf*)leaf)->snapid;
}


/*set the snapid of leaf*/
static inline void set_leaf_snapid(void* leaf, uint32_t snapid)
{
    ((art_leaf*)leaf)->snapid = snapid;
}


/*get the node_type of leaf*/
static inline uint8_t get_leaf_node_type(void* leaf)
{
    return ((art_leaf*)leaf)->node_type;
}


/*set the node_type of leaf*/
static inline void set_leaf_node_type(void* leaf, uint8_t node_type)
{
    ((art_leaf*)leaf)->node_type = node_type;
}


/*get the op_type of leaf*/
static inline uint8_t get_leaf_op_type(void* leaf)
{
    return ((art_leaf*)leaf)->op_type;
}


/*set the op_type of leaf*/
static inline void set_leaf_op_type(void* leaf, uint8_t op_type)
{
    ((art_leaf*)leaf)->op_type = op_type;
}


/*get the value of leaf*/
static inline uint8_t* get_leaf_value(void* leaf)
{
    return ((art_leaf*)leaf)->key_value;
}


/*get the value length of leaf*/
static inline uint32_t get_leaf_value_len(void* leaf)
{
    return ((art_leaf*)leaf)->value_len;
}


/* set value of leaf */
static inline void set_leaf_value(void *leaf, uint8_t *value, uint32_t value_len)
{
    art_leaf *l                 = (art_leaf *)leaf;

    ART_MEMCPY(l->key_value, value, value_len);
    l->value_len = value_len;
}

/* set leaf key */
static inline void set_leaf_key(void *leaf, uint8_t *key, uint32_t key_len)
{
    art_leaf* l = (art_leaf *)leaf;

    ART_MEMCPY(l->key_value + get_leaf_value_len(leaf), key, key_len);
}

/* get leaf key */
static inline uint8_t* get_leaf_key(void* leaf)
{
    return ((art_leaf*)leaf)->key_value + get_leaf_value_len(leaf);
    
}

/* set leaf key length */
static inline void set_leaf_key_len(void *leaf, uint32_t key_len)
{
    art_leaf* l = (art_leaf *)leaf;

    l->key_len = key_len;
}


/* get leaf key length */
static inline uint32_t get_leaf_key_len(void* leaf)
{
    return ((art_leaf*)leaf)->key_len;
}

/* check the key valid in leaf */
static inline bool check_leaf_key_valid(void *leaf, uint8_t *key, uint32_t key_len)
{
    if (get_leaf_key_len(leaf) != key_len)
    {
        return false;
    }
    
    if (0 != ART_MEMCMP(get_leaf_key(leaf), key, key_len))
    {
        return false;
    }
    return true;
}

#endif




#if DESC("fix length leaf ops")
/*calc leaf of fix value size*/
static inline uint32_t calc_svalue_leaf_size(IN uint32_t value_len, IN uint8_t verify_key_len)
{
    return sizeof(art_svalue_leaf) + sizeof(uint8_t) * (uint32_t)(value_len + verify_key_len);
}


/*get the snapid of svalue leaf*/
static inline uint32_t get_svalue_leaf_snapid(void* leaf)
{
    return ((art_svalue_leaf*)leaf)->snapid;
}


/*set the snapid of svalue leaf*/
static inline void set_svalue_leaf_snapid(void* leaf, uint32_t snapid)
{
    ((art_svalue_leaf*)leaf)->snapid = snapid;
}



/*get the node_type of svalue leaf*/
static inline uint8_t get_svalue_leaf_node_type(void* leaf)
{
    return ((art_svalue_leaf*)leaf)->node_type;
}


/*set the node_type of svalue leaf*/
static inline void set_svalue_leaf_node_type(void* leaf, uint8_t node_type)
{
    ((art_svalue_leaf*)leaf)->node_type = node_type;
}


/*get the op_type of svalue leaf*/
static inline uint8_t get_svalue_leaf_op_type(void* leaf)
{
    return ((art_svalue_leaf*)leaf)->op_type;
}


/*set the op_type of svalue leaf*/
static inline void set_svalue_leaf_op_type(void* leaf, uint8_t op_type)
{
    ((art_svalue_leaf*)leaf)->op_type = op_type;
}


/*get the value of svalue leaf*/
static inline uint8_t* get_svalue_leaf_value(void* leaf)
{
    return ((art_svalue_leaf*)leaf)->key_value;
}


/*get the value length of svalue leaf*/
static inline uint32_t get_svalue_leaf_value_len(void* leaf)
{
    //return t->value_len;
    return ((art_svalue_leaf*)leaf)->value_len;
}


/* set value of svalue leaf */
static inline void set_svalue_leaf_value(void *leaf, uint8_t *value, uint32_t value_len)
{
    art_svalue_leaf *l                 = (art_svalue_leaf *)leaf;

    ART_MEMCPY(l->key_value, value, value_len);
}

/* get svalue leaf key */
static inline void set_svalue_leaf_key(void* leaf, uint8_t *key, uint32_t key_len)
{
    art_svalue_leaf* l = (art_svalue_leaf *)leaf;

    ART_MEMCPY(l->key_value + get_svalue_leaf_value_len(leaf), key, key_len);
}


/* get svalue leaf key */
static inline uint8_t* get_svalue_leaf_key(void* leaf)
{
    return ((art_svalue_leaf*)leaf)->key_value + get_svalue_leaf_value_len(leaf);
}


/* get svalue leaf key length */
static inline uint32_t get_svalue_leaf_key_len(void* leaf)
{
    return ARTREE_FUISON_LEAF_VERIFY_LEN;
}

/* check the key valid in svalue leaf */
static inline bool check_svalue_leaf_key_valid(void *leaf, uint8_t *key, uint32_t key_len)
{
    if (get_svalue_leaf_key_len(leaf) != key_len)
    {
        return false;
    }
    
    if (0 != ART_MEMCMP(get_svalue_leaf_key(leaf), key, key_len))
    {
        return false;
    }
    return true;
}

#endif

#ifdef __cplusplus
}
#endif
#endif

