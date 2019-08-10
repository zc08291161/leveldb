#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "art_log.h"
#include "art_snapshot_memtable.h"
#ifndef _SCM_PCLINT_
#include "art_epoch.h"
#endif
#ifdef ARTREE_DEBUG
#include "art_util.h"
#endif

//MODULE_ID(PID_ART);

#ifdef __cplusplus
extern "C" {
#endif

/****************************************************************************/
#if DESC("destroy memtable node")

static void destroy_node256(
    IN art_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                         = 0;
    memtable_art_node256* node256 = (memtable_art_node256*)node;
    
    for (i = 0; i < 256; i++)
    {
        if (node256->children[i].mem_addr)
        { destroy_node(node256->children[i].mem_addr, arena_destroy_callback); }
    }
}

static void destroy_node48(
    IN art_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                       = 0;
    memtable_art_node48* node48 = (memtable_art_node48*)node;
    
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node48->children[i].mem_addr, arena_destroy_callback);
    }
}

static void destroy_node16(
    IN art_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                       = 0;
    memtable_art_node16* node16 = (memtable_art_node16*)node;
    
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node16->children[i].mem_addr, arena_destroy_callback);
    }
}

static void destroy_node4(
    IN art_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                     = 0;
    memtable_art_node4* node4 = (memtable_art_node4*)node;
    
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node4->children[i].mem_addr, arena_destroy_callback);
    }
}

// Recursively destroys the tree
void destroy_node(
    IN void* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    /* in_param init */
    if (!node) { return; }

    /* Handle each node type */
    switch (((art_node*)node)->node_type)
    {
        case MEMTABLE_ART_NODE4:
            destroy_node4((art_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_NODE16:
            destroy_node16((art_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_NODE48:
            destroy_node48((art_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_NODE256:
            destroy_node256((art_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_SNAPNODE4:
            destroy_snap_node4((art_snap_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_SNAPNODE16:            
            destroy_snap_node16((art_snap_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_SNAPNODE48:
            destroy_snap_node48((art_snap_node*)node, arena_destroy_callback);
            break;

        case MEMTABLE_ART_SNAPNODE256:
            destroy_snap_node256((art_snap_node*)node, arena_destroy_callback);
            break;
            
        case MEMTABLE_ART_LEAF:
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", ((art_node*)node)->node_type);
            ART_ASSERT(0);
            break;
    }
    
#ifndef SUPPORT_RW_CONCURRENCY
    // Free ourself on the way up
    arena_destroy_callback(node);

#else
    // Free ourself on the way up
    arena_destroy_callback(GET_EPOCH_HEAD(node, sizeof(epoch_head_t)));

#endif
}

#endif

/**
 * get current node's end child
 * @arg node The current node
 * @return NULL means end child is NULL.
 */
void** memtale_get_end_child(IN art_node* node)
{
    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            return (void**)&(((memtable_art_node4*)node)->end.mem_addr);

        case MEMTABLE_ART_NODE16:
            return (void**)&(((memtable_art_node16*)node)->end.mem_addr);

        case MEMTABLE_ART_NODE48:
            return (void**)&(((memtable_art_node48*)node)->end.mem_addr);

        case MEMTABLE_ART_NODE256:
            return (void**)&(((memtable_art_node256*)node)->end.mem_addr);

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", node->node_type);            
            ART_ASSERT(0);
            break;
    }

    return NULL; /*lint !e831*/
}

/*memtable set the child, notice node48 is different*/
void memtable_node_set_child(
    IN art_node* node,
    IN int32_t idx,
    IN void* child)
{
    union
    {
        memtable_art_node4* p1;
        memtable_art_node16* p2;
        memtable_art_node48* p3;
        memtable_art_node256* p4;
    }p;
    
    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:            
            p.p1 = (memtable_art_node4*)node;
            p.p1->children[idx].mem_addr = child;
            break;
            
        case MEMTABLE_ART_NODE16:
            p.p2 = (memtable_art_node16*)node;
            p.p2->children[idx].mem_addr = child;
            break;
            
        case MEMTABLE_ART_NODE48:            
            p.p3 = (memtable_art_node48*)node;
            p.p3->children[p.p3->keys[idx] - 1].mem_addr = child;
            break;

        case MEMTABLE_ART_NODE256:
            p.p4 = (memtable_art_node256*)node;
            p.p4->children[idx].mem_addr = child;
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", node->node_type);            
            ART_ASSERT(0);
            break;
    }

    return;
}

/**
 * set current node's end child
 * @arg node The current node
 * @return void.
 */
void memtable_node_set_end_child(
    IN art_node* node, 
    IN void* child)
{
    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            ((memtable_art_node4*)node)->end.mem_addr = child;
            break;

        case MEMTABLE_ART_NODE16:
            ((memtable_art_node16*)node)->end.mem_addr = child;
            break;

        case MEMTABLE_ART_NODE48:
            ((memtable_art_node48*)node)->end.mem_addr = child;
            break;

        case MEMTABLE_ART_NODE256:
            ((memtable_art_node256*)node)->end.mem_addr = child;
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", node->node_type);               
            ART_ASSERT(0);
            break;
    }

    return; /*lint !e831*/
}


bool check_art_memtable_node256_child_vaild(art_node* node, uint8_t child_index)
{
    memtable_art_node256* memtable_node = (memtable_art_node256*) node;

    return (memtable_node->children[child_index].mem_addr != NULL);
}


/**
 * find a child
 * @arg t The tree
 * @arg node The parent node
 * @arg c A byte of the key
 * @arg child_idx The index of child
 * @return the child.
 */
art_node** memtable_node_find_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT int* child_idx)
{
    int32_t  i        = 0;
    uint32_t mask     = 0;
    uint32_t bitfield = 0;
    union
    {
        memtable_art_node4* p1;
        memtable_art_node16* p2;
        memtable_art_node48* p3;
        memtable_art_node256* p4;
    } p;

    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            p.p1 = (memtable_art_node4*)node;

            for (i = 0 ; i < node->num_children; i++)
            {
                /* this cast works around a bug in gcc 5.1 when unrolling loops
                * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
                */
                if (p.p1->keys[i] == c)
                {
                    *child_idx = i;
                    return (art_node**)&(p.p1->children[i].mem_addr);
                }
            }

            break;

        case MEMTABLE_ART_NODE16:
            p.p2 = (memtable_art_node16*)node;

            // support non-86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION
            // Compare the key to all 16 stored keys
            __m128i cmp;
            cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)p.p2->keys)); /*lint !e746 !e718 !e63 !e26*/
            // Use a mask to ignore children that don't exist
            mask = (1 << node->num_children) - 1;
            bitfield = _mm_movemask_epi8(cmp) & mask;/*lint !e746 !e718*/
#else
            // Compare the key to all 16 stored keys
            bitfield = 0;

            for (i = 0; i < 16; ++i)
            {
                if (p.p2->keys[i] == c)
                { bitfield |= (1 << i); }
            }

            // Use a mask to ignore children that don't exist
            mask = (1 << node->num_children) - 1;
            bitfield &= mask;
#endif

            /*
             * If we have a match (any bit set) then we can
             * return the pointer match using ctz to get
             * the index.
             */
            if (bitfield)
            {
                *child_idx = __builtin_ctz(bitfield); /*lint !e746 !e718*/            
                return (art_node**)&(p.p2->children[__builtin_ctz(bitfield)].mem_addr);
            }

            break;

        case MEMTABLE_ART_NODE48:
            p.p3 = (memtable_art_node48*)node;
            i = p.p3->keys[c];

            if (i)
            {        
                *child_idx = c;             
                return (art_node**)&(p.p3->children[i-1].mem_addr);
            }

            break;

        case MEMTABLE_ART_NODE256:
            p.p4 = (memtable_art_node256*)node;

            if (check_art_memtable_node256_child_vaild(node, c))
            {        
                *child_idx = c;             
                return (art_node**)&(p.p4->children[c].mem_addr);
            }

            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", node->node_type);               
            ART_ASSERT(0);
            break;
    }

    return NULL; /*lint !e831*/
}


uint32_t get_memtable_node_type_size(uint8_t node_type)
{
    uint32_t size = 0;
    
    switch (node_type)
    {
        case MEMTABLE_ART_NODE4:
            size = sizeof(memtable_art_node4);
            break;

        case MEMTABLE_ART_NODE16:
            size = sizeof(memtable_art_node16);
            break;

        case MEMTABLE_ART_NODE48:
            size = sizeof(memtable_art_node48);
            break;

        case MEMTABLE_ART_NODE256:
            size = sizeof(memtable_art_node256);
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", node_type);                
            ART_ASSERT(0);
            break;
    }

    return size;
}

#if DESC("memtable add child")

/*add 256_node to new node*/
static art_node** add_child256_in_new_node(
    IN memtable_art_node256* node,
    IN uint8_t c,
    IN void* child)
{
    node->n.num_children++;
    node->children[c].mem_addr = (void*)child;
    return (art_node**) & (node->children[c].mem_addr);
}


/*add 48_node to new node*/
static art_node** add_child48_in_new_node(
    IN memtable_art_node48* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child)
{
    uint8_t pos = 0;

    assert(node->n.num_children < 48);
    
    /*while (node->children[pos].mem_addr)
    {
        pos++;
    }*/

    pos = (uint8_t)node->n.num_children;
    node->children[pos].mem_addr = (void*)child;
    node->keys[c] = pos + 1;
    node->n.num_children++;
    
    return (art_node**)&(node->children[pos].mem_addr);
} /*lint !e715*/

/*add 16_node to new node*/
static art_node** add_child16_in_new_node(
    IN memtable_art_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child)
{
    unsigned idx;
    unsigned mask;
    unsigned bitfield = 0;

    assert(node->n.num_children < 16);
    
    {
        mask = (1 << node->n.num_children) - 1;

        // support non-x86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION

        __m128i keys = _mm_loadu_si128((__m128i*)node->keys);
        __m128i new_key = _mm_set1_epi8(c);
        __m128i new_keys = _mm_max_epu8(new_key, keys);
        __m128i result = _mm_cmpeq_epi8(new_keys, keys);

        bitfield = _mm_movemask_epi8(result) & mask; /*lint !e737*/
#else 
        // Compare the key to all 16 stored keys
        for (uint32_t i = 0; i < node->n.num_children; ++i)
        {
            if (c < node->keys[i])
            {
                bitfield |= (1 << i);
            }
        }

        // Use a mask to ignore children that don't exist
        bitfield &= mask;
#endif
        // Check if less than any
        if (bitfield)
        {
            /*not less than any, shift a room*/
            idx = __builtin_ctz(bitfield);  /*lint !e516 !e732*/
            ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, node->n.num_children - idx);
            ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
        }
        else
        {
            /*add to last*/
            idx = node->n.num_children;
        }

        /* Set the child */
        node->keys[idx] = c;
        node->children[idx].mem_addr = (void*)child;
        node->n.num_children++;
        
        return (art_node**) & (node->children[idx].mem_addr);
    }
} /*lint !e715*/

/*add a 4_node to new node*/
art_node** add_child_in_new_node(
    IN void* parent_node,
    IN uint8_t c,
    IN void* child)
{
    uint32_t idx             = 0;
    memtable_art_node4* node = (memtable_art_node4*)parent_node;
    
    ART_ASSERT(node->n.num_children < 4);

    for (idx = 0; idx < node->n.num_children; idx++)
    {
        if (c < node->keys[idx])
        {
            break;
        }
    }
    if(idx != node->n.num_children)
    {
        /* Shift to make room */
        ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));
        ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
    }
    /* Insert element */
    node->keys[idx] = c;
    node->children[idx].mem_addr = (void*)child;
    node->n.num_children++;
    return (art_node**)&(node->children[idx].mem_addr);
} /*lint !e715*/

/**
 * Add a child
 * @arg t The tree
 * @arg node The parent node
 * @arg ref The reference of the node
 * @arg c The byte of the key
 * @arg child The node to be added
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
art_node** add_child256(
    IN memtable_art_node256* node,
    IN uint8_t c,
    IN void* child)
{
#ifndef SUPPORT_RW_CONCURRENCY

    node->n.num_children++;
    node->children[c].mem_addr = (void*)child;
    return (art_node**) & (node->children[c].mem_addr);

#else

    node->n.num_children++;
    /*
    __sync_synchronize();// node256: no need to add memory barrier
    */
    node->children[c].mem_addr = (void*)child;
    return (art_node**) & (node->children[c].mem_addr);
#endif
}

/**
 * Add a child
 * @arg t The tree
 * @arg node The parent node
 * @arg ref The reference of the node
 * @arg c The byte of the key
 * @arg child The node to be added
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
art_node** add_child48(
    IN art_tree_t* t,
    IN memtable_art_node48* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    memtable_art_node256* new_node256 = NULL;
    int i                             = 0;
    art_node** ret                    = NULL;
    uint8_t    pos                    = 0;

    if (node->n.num_children < 48)
    {

        while (node->children[pos].mem_addr)
        {
            pos++;
        }
        
#ifndef SUPPORT_RW_CONCURRENCY

        node->children[pos].mem_addr = (void*)child;
        node->keys[c] = pos + 1;
        node->n.num_children++;
        return (art_node**) & (node->children[pos].mem_addr);
#else
        node->children[pos].mem_addr = (void*)child;

        ART_MEMORY_BARRIER(); /*lint !e746 !e718*/ // node48:the keys must be changed after children(for search)

        node->keys[c] = pos + 1;
        node->n.num_children++;
        return (art_node**) & (node->children[pos].mem_addr);
#endif
    }
    else
    {
        new_node256 = (memtable_art_node256*)alloc_node(t, MEMTABLE_ART_NODE256, arena_alloc_callback, args);
        if (new_node256 == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node256[%p] fail when add child node.", new_node256);
            return NULL;
        }

        for (i = 0; i < 256; i++)
        {
            if (node->keys[i])
            {
                new_node256->children[i].mem_addr = node->children[node->keys[i] - 1].mem_addr;
            }
        }

        memtable_node_copy_header((art_node*)new_node256, (art_node*)node);
        new_node256->end = node->end;
        new_node256->max_snapid = node->max_snapid;
#ifndef SUPPORT_RW_CONCURRENCY
        *ref = (art_node*)new_node256;
        reset_free_node(t, (art_node*)node);
        return add_child256_in_new_node(new_node256, c, child);
#else
        ret = add_child256_in_new_node(new_node256, c, child);

        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node256);

        art_mem_reclamation(t, (art_node*)node);
        
        return ret;
#endif
    }
}

static art_node** add_child16_cow(
    IN unsigned bitfield, 
    IN art_tree_t* t,
    IN memtable_art_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    unsigned idx                    = 0;
    memtable_art_node16* new_node16 = NULL;
    
    //  need to move children,cow
    idx = __builtin_ctz(bitfield); /*lint !e516 !e732*/

    new_node16 = (memtable_art_node16*)alloc_node(t, MEMTABLE_ART_NODE16, arena_alloc_callback, args);
    if (new_node16  == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc node16[%p] fail when add child node.", new_node16);
        return NULL;
    }
         
    ART_MEMCPY(new_node16->children, node->children, sizeof(memtable_art_child_t)*idx);
    ART_MEMMOVE(new_node16->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
    ART_MEMCPY(new_node16->keys, node->keys, sizeof(uint8_t)*idx);
    ART_MEMMOVE(new_node16->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));

    memtable_node_copy_header((art_node*)new_node16, (art_node*)node);

    // Set the child
    new_node16->keys[idx] = c;
    new_node16->children[idx].mem_addr = (void*)child;
    new_node16->n.num_children++;
    new_node16->end = node->end;
    new_node16->max_snapid = node->max_snapid;
    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node16);
    
    art_mem_reclamation(t, (art_node*)node);
    
    return (art_node**) & (new_node16->children[idx].mem_addr);
}

static art_node** add_child16_to_child48(
    IN art_tree_t* t,
    IN memtable_art_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    int i                           = 0;
    memtable_art_node48* new_node48 = NULL;
    art_node** ret                  = NULL;
    
    new_node48 = (memtable_art_node48*)alloc_node(t, MEMTABLE_ART_NODE48, arena_alloc_callback, args);
    if (new_node48 == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "add_child16_to_child48 alloc node48[%p] fail when add child node.", new_node48);
        return NULL;
    }

    // Copy the child pointers and populate the key map
    //ART_MEMCPY(new_node48->children, node->children, sizeof(memtable_art_child_t)*node->n.num_children);
    ART_MEMCPY(new_node48->children, node->children, sizeof(memtable_art_child_t)*node->n.num_children);
    for (i = 0; i < node->n.num_children; i++)
    {
        new_node48->keys[node->keys[i]] = (uint8_t)(i + 1);
    }

    memtable_node_copy_header((art_node*)new_node48, (art_node*)node);
    new_node48->end = node->end;
    new_node48->max_snapid = node->max_snapid;
#ifndef SUPPORT_RW_CONCURRENCY
        
    *ref = (art_node*)new_node48;
    reset_free_node(t, (art_node*)node);
    return add_child48_in_new_node(new_node48, ref, c, child);

#else

    ret = add_child48_in_new_node(new_node48, ref, c, child);

    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node48);

    art_mem_reclamation(t, (art_node*)node);
    
    return ret;
#endif
    
}
/**
 * Add a child
 * @arg t The tree
 * @arg node The parent node
 * @arg ref The reference of the node
 * @arg c The byte of the key
 * @arg child The node to be added
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
art_node** add_child16(
    IN art_tree_t* t,
    IN memtable_art_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    unsigned idx;
    unsigned mask;
    unsigned bitfield = 0;

    if (node->n.num_children < 16)
    {
        mask = (1 << node->n.num_children) - 1;

        // support non-x86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION

        __m128i keys = _mm_loadu_si128((__m128i*)node->keys);
        __m128i new_key = _mm_set1_epi8(c);
        __m128i new_keys = _mm_max_epu8(new_key, keys);
        __m128i result = _mm_cmpeq_epi8(new_keys, keys);

        bitfield = _mm_movemask_epi8(result) & mask; /*lint !e737*/
#else 
        // Compare the key to all 16 stored keys
        for (int i = 0; i < node->n.num_children; ++i)
        {
            if (c < node->keys[i])
            {
                bitfield |= (1 << i);
            }
        }

        // Use a mask to ignore children that don't exist
        bitfield &= mask;
#endif

#ifndef SUPPORT_RW_CONCURRENCY

        // Check if less than any
        if (bitfield)
        {
            idx = __builtin_ctz(bitfield);
            ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, node->n.num_children - idx);
            ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
        }
        else
        {
            idx = node->n.num_children;
        }

        // Set the child
        node->keys[idx] = c;
        node->children[idx].mem_addr = (void*)child;
        node->n.num_children++;
        return (art_node**) & (node->children[idx].mem_addr);
#else
        if (bitfield)
        {
            //  need to move children,cow
            return add_child16_cow(bitfield, t, node, ref, c, child, arena_alloc_callback, args);
        }
        else
        { 
            //  no need to move children
            idx = node->n.num_children; 
            
            node->keys[idx] = c;
            node->children[idx].mem_addr = (void*)child;

            ART_MEMORY_BARRIER();// node16:the num_children must be changed after key and children(for search)

            node->n.num_children++;
            return (art_node**) & (node->children[idx].mem_addr);
        }
#endif
    }
    else
    {
        return add_child16_to_child48(t, node, ref, c, child, arena_alloc_callback, args);
    }
}

/**
 * Add a child
 * @arg t The tree
 * @arg node The parent node
 * @arg ref The reference of the node
 * @arg c The byte of the key
 * @arg child The node to be added
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
art_node** add_child4(
    IN art_tree_t* t,
    IN memtable_art_node4* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    memtable_art_node4* new_node4   = NULL;
    memtable_art_node16* new_node16 = NULL;
    art_node** ret                  = NULL;
    uint32_t idx                    = 0;

    if (node->n.num_children < 4)
    {
        for (idx = 0; idx < node->n.num_children; idx++)
        {
            if (c < node->keys[idx])
            {
                break;
            }
        }
        
#ifndef SUPPORT_RW_CONCURRENCY

        // Shift to make room
        ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));
        ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));

        // Insert element
        node->keys[idx] = c;
        node->children[idx].mem_addr = (void*)child;
        node->n.num_children++;
        return (art_node**) & (node->children[idx].mem_addr);
        
#else
        if (idx == node->n.num_children)
        {
            // if no need move key and children, directly insert,no cow
            
            node->keys[idx] = c;
            node->children[idx].mem_addr = (void*)child;
            
            ART_MEMORY_BARRIER();// node4:the num_children must be changed after key and children(for search)
            
            node->n.num_children++;
            return (art_node**) & (node->children[idx].mem_addr);
        }
        else
        {
            new_node4 = (memtable_art_node4*)alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
            if (new_node4  == NULL)
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "alloc node4[%p] fail when add child node.", new_node4);
                return NULL;
            }
            
            ART_MEMCPY(new_node4->children, node->children, sizeof(memtable_art_child_t)*idx);
            ART_MEMCPY(new_node4->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
            
            ART_MEMCPY(new_node4->keys, node->keys, sizeof(uint8_t)*idx);
            ART_MEMCPY(new_node4->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));
            
            memtable_node_copy_header((art_node*)new_node4, (art_node*)node);
            new_node4->keys[idx] = c;
            new_node4->children[idx].mem_addr = (void*)child;
            new_node4->n.num_children++;
            new_node4->end = node->end;
            new_node4->max_snapid = node->max_snapid;
            ART_MEMORY_BARRIER();
            ART_ATOMIC_SET_PTR(ref, new_node4);
            
            art_mem_reclamation(t, (art_node*)node);
            
            return (art_node**) & (new_node4->children[idx].mem_addr);
        }   
#endif
    }
    else
    {
        /*the 4_node if full then expand to 16_node*/
        new_node16 = (memtable_art_node16*)alloc_node(t, MEMTABLE_ART_NODE16, arena_alloc_callback, args);
        if (new_node16 == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node16[%p] fail when add child node.", new_node16);
            return NULL;
        }

        // Copy the child pointers and the key map
        ART_MEMCPY(new_node16->children, node->children, sizeof(memtable_art_child_t)*node->n.num_children);
        ART_MEMCPY(new_node16->keys, node->keys, sizeof(uint8_t)*node->n.num_children);
        memtable_node_copy_header((art_node*)new_node16, (art_node*)node);
        new_node16->end = node->end;
        new_node16->max_snapid = node->max_snapid;
#ifndef SUPPORT_RW_CONCURRENCY
        
        *ref = (art_node*)new_node16;
        reset_free_node(t, (art_node*)node);
        return add_child16_in_new_node(new_node16, ref, c, child);
#else

        ret = add_child16_in_new_node(new_node16, ref, c, child);

        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node16);

        art_mem_reclamation(t, (art_node*)node);
        
        return ret;
#endif

    }
}

/**
 * Add a child
 * @arg t The tree
 * @arg node The parent node
 * @arg ref The reference of the node
 * @arg c The byte of the key
 * @arg child The node to be added
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
art_node** add_child(
    IN art_tree_t* t,
    IN void* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    switch (((art_node*)node)->node_type)
    {
        case MEMTABLE_ART_NODE4:
            return add_child4(t, (memtable_art_node4*)node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_SNAPNODE4:
            return (art_node**)(void*)add_snap_child4(t, node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_NODE16:
            return add_child16(t, (memtable_art_node16*)node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_SNAPNODE16:
            return (art_node**)(void*)add_snap_child16(t, node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_NODE48:
            return add_child48(t, (memtable_art_node48*)node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_SNAPNODE48:
            return (art_node**)(void*)add_snap_child48(t, node, ref, c, child, arena_alloc_callback, args);

        case MEMTABLE_ART_NODE256:
            return add_child256((memtable_art_node256*)node, c, child);

        case MEMTABLE_ART_SNAPNODE256:
            return (art_node**)(void*)add_snap_child256(node, c, child);

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", ((art_node*)node)->node_type);             
            ART_ASSERT(0);
            break;
    }

    return NULL; /*lint !e527*/
}

#endif

void memtable_new_node_set_partial(art_node* memtable_node4, uint8_t* partial, uint32_t prefix_diff)
{
    memtable_art_node4* node = (memtable_art_node4*)memtable_node4;
    node->n.partial_len      = (uint8_t)prefix_diff;
    ART_MEMCPY(node->n.partial, partial, (uint32_t)prefix_diff);
}

void memtable_new_node_set_end_valid(art_node* memtable_node4, uint8_t flag)
{
    memtable_art_node4* node = (memtable_art_node4*)memtable_node4;
    node->n.end_valid        = flag;
}


uint8_t memtable_node_get_key(art_node* node, int key_idx)
{
    union
    {
        memtable_art_node4* p1;
        memtable_art_node16* p2;
        memtable_art_node48* p3;
        memtable_art_node256* p4;
    }p;

    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:            
            p.p1 = (memtable_art_node4*)node;
            return p.p1->keys[key_idx];

        case MEMTABLE_ART_NODE16:
            p.p2 = (memtable_art_node16*)node;
            return p.p2->keys[key_idx];

        case MEMTABLE_ART_NODE48:            
            return (uint8_t)key_idx;

        case MEMTABLE_ART_NODE256:
            return (uint8_t)key_idx;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", node->node_type);            
            ART_ASSERT(0);
            return kIOError;
    }
    
    return kOk;
            
}

#if DESC("memtable search node equal or bigger")

static int32_t memtable_find_node4_child_equal_or_bigger(
    IN memtable_art_node4* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = 0;
    
    while(i < node->n.num_children && node->keys[i] < c)
    {
        ++i;
    }


    if(i == node->n.num_children)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


static int32_t memtable_find_node16_child_equal_or_bigger(
    IN memtable_art_node16* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = 0;
    
    while(i < node->n.num_children && node->keys[i] < c)
    {
        ++i;
    }


    if(i == node->n.num_children)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


static int32_t memtable_find_node48_child_equal_or_bigger(
    IN memtable_art_node48* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = c;
    
    while(i < MAX_CHILDREN_NUM && 0 == node->keys[i])
    {
        ++i;
    }


    if(i == MAX_CHILDREN_NUM)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_node *)node->children[node->keys[i] - 1].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[c] != 0)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


static int32_t memtable_find_node256_child_equal_or_bigger(
    IN memtable_art_node256* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = c;
    
    while(i < MAX_CHILDREN_NUM && NULL == node->children[i].mem_addr)
    {
        ++i;
    }


    if(i == MAX_CHILDREN_NUM)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(NULL != node->children[c].mem_addr)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}

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
int32_t memtable_search_node_equal_or_bigger(
    IN  art_node*  parent_node,
    IN  uint8_t* key,
	IN  uint32_t key_len,
	IN  uint32_t depth,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int same_len     = 0;
    int32_t result   = kOk;
    
    //assert(is_art_inner_node(parent_node));
    
    same_len = check_prefix(parent_node, key, key_len, depth);
    depth += same_len;
    if(depth == key_len)
    {
        result = memtable_get_node_left_child(parent_node, child, idx_in_parent);
        assert(kOk == result);

        return (parent_node->partial_len == same_len && END_POSITION_INDEX == *idx_in_parent) ? ART_COMPARE_RESULT_EQUAL : ART_COMPARE_RESULT_GREATER;
    }

    if(same_len != parent_node->partial_len && parent_node->partial[same_len] < key[depth])
    {
        return ART_COMPARE_RESULT_LESS;
    }

    if(same_len != parent_node->partial_len && parent_node->partial[same_len] > key[depth])
    {
        result = memtable_get_node_left_child(parent_node, child, idx_in_parent);
        assert(kOk == result);

        return ART_COMPARE_RESULT_GREATER;
    }

    // finally, same_len == parent_node->partial_len
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            result = memtable_find_node4_child_equal_or_bigger((void*)parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE16:
            result = memtable_find_node16_child_equal_or_bigger((void*)parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE48:
            result = memtable_find_node48_child_equal_or_bigger((void*)parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE256:
            result = memtable_find_node256_child_equal_or_bigger((void*)parent_node, key[depth], child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            break;
    }

    return result;
}

#endif

#if DESC("memtable search node equal")


static int32_t memtable_find_node4_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = 0;
    memtable_art_node4* node4   = (memtable_art_node4*)node;

    for (i = 0; i < node->num_children; i++)
    {
        if (node4->keys[i] == c)
        {
            *idx_in_parent = i;
            *child = (art_node*)node4->children[i].mem_addr;
            return kOk;
        }
        if (node4->keys[i] > c)
        {
            break;
        }
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable_find_node4_child not find child in node4.");
    return kNotFound;
}


static int32_t memtable_find_node16_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int      i        = 0;
    uint32_t mask     = 0;
    uint32_t bitfield = 0;
    memtable_art_node16* node16 = (memtable_art_node16*)node;

    // support non-86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION
    // Compare the key to all 16 stored keys
    __m128i cmp;
    cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)node16->keys)); /*lint !e746 !e718 !e63 !e26*/
                                                                                   // Use a mask to ignore children that don't exist
    mask = (1 << node->num_children) - 1;
    bitfield = _mm_movemask_epi8(cmp) & mask;/*lint !e746 !e718*/
#else
    // Compare the key to all 16 stored keys
    bitfield = 0;

    for (i = 0; i < node->num_children; ++i)
    {
        if (node16->keys[i] == c)
        {
            bitfield |= (1 << i);
            break;
        }
        if (node16->keys[i] > c)
        {
            break;
        }
    }

    // Use a mask to ignore children that don't exist
    mask = (1 << node->num_children) - 1;
    bitfield &= mask;
#endif

    /*
    * If we have a match (any bit set) then we can
    * return the pointer match using ctz to get
    * the index.
    */
    if (bitfield)
    {
        *idx_in_parent = __builtin_ctz(bitfield); /*lint !e746 !e718*/
        *child = (art_node*)node16->children[__builtin_ctz(bitfield)].mem_addr;
        return kOk;
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable_find_node16_child not find child in node16.");
    return kNotFound;
}


static int32_t memtable_find_node48_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    memtable_art_node48* node48 = (memtable_art_node48*)node;
    uint32_t idx = node48->keys[c];
    if(0 == idx || NULL == node48->children[idx - 1].mem_addr)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable_find_node48_child not find child in node48, the idx[%d] is zero.", idx);
        return kNotFound;
    }

    *child = (art_node *)node48->children[idx - 1].mem_addr;
    *idx_in_parent = c;
    return kOk;
}

static int32_t memtable_find_node256_child(
    IN art_node* node, 
    IN uint8_t c,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
    memtable_art_node256* node256 = (memtable_art_node256*)node;
    if(NULL == node256->children[c].mem_addr)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable_find_node256_child not find child in node256, the node[%p] on idx[%d] pos is NULL.", 
                                node256->children[c].mem_addr, c);        
        return kNotFound;
    }

    *child = (art_node *)node256->children[c].mem_addr;
    *idx_in_parent = c;
    return kOk;
}

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
    OUT uint32_t*  idx_in_parent)
{
    int same_len          = 0;
    int32_t result      = kNotFound;
    void **child_node     = NULL;
    
    same_len = check_prefix(parent_node, key, key_len, depth);
    if(same_len != parent_node->partial_len)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable search node fail, the prefix(same_len[%d], node->partial_len[%d]) is not match.",
                                same_len, parent_node->partial_len);
        return kNotFound;
    }

    depth += same_len;
    if(depth == key_len)
    {
        child_node = memtale_get_end_child(parent_node);
        *child = (art_node *)(*child_node);
        *idx_in_parent = END_POSITION_INDEX;

        return NULL == *child ? kNotFound : kOk;
    }

    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            result = memtable_find_node4_child(parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE16:
            result = memtable_find_node16_child(parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE48:
            result = memtable_find_node48_child(parent_node, key[depth], child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_NODE256:
            result = memtable_find_node256_child(parent_node, key[depth], child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            ART_ASSERT(0);
            result = kIOError;
            break;
    }

    return result;
}

#endif

uint8_t memtable_node_get_key_by_idx(
    IN art_node* memtable_node, 
    IN int idx)
{
    uint8_t key                     = 0;
    union
    {
        memtable_art_node4* p1;
        memtable_art_node16* p2;
        memtable_art_node48* p3;
        memtable_art_node256* p4;
    }p;

    switch (memtable_node->node_type)
    {
        case MEMTABLE_ART_NODE4:            
            p.p1 = (memtable_art_node4*)memtable_node;
            key = p.p1->keys[idx];
            break;

        case MEMTABLE_ART_NODE16:
            p.p2 = (memtable_art_node16*)memtable_node;
            key = p.p2->keys[idx];
            break;

        case MEMTABLE_ART_NODE48:            
        case MEMTABLE_ART_NODE256:
            key = (uint8_t)idx;
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", memtable_node->node_type);              
            ART_ASSERT(0);
            break;
    }

    return key;
}

void* memtable_node_get_child_by_child_idx(
    IN art_node* node, 
    IN int idx)
{
    void* child = NULL;

    union
    {
        memtable_art_node4* p1;
        memtable_art_node16* p2;
        memtable_art_node48* p3;
        memtable_art_node256* p4;
    }p;

    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:            
            p.p1 = (memtable_art_node4*)node;
            child = p.p1->children[idx].mem_addr;
            break;
            
        case MEMTABLE_ART_NODE16:
            p.p2 = (memtable_art_node16*)node;
            child = p.p2->children[idx].mem_addr;
            break;
            
        case MEMTABLE_ART_NODE48:            
            p.p3 = (memtable_art_node48*)node;
            child = p.p3->children[idx].mem_addr;
            break;

        case MEMTABLE_ART_NODE256:
            p.p4 = (memtable_art_node256*)node;
            child = p.p4->children[idx].mem_addr;
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", node->node_type);              
            ART_ASSERT(0);
            break;
    }

    return child;
}

#if DESC("memtable get next valid child idx")

static inline int32_t memtable_node4_get_next_valid_child_idx(
    IN art_node* node,
    IN int32_t idx, 
    OUT uint32_t *key_idx)
{
    int32_t next_idx = idx + 1;
    if (next_idx >= node->num_children)
    {
        return MAX_CHILDREN_NUM;
    }
    *key_idx = next_idx;
    return next_idx;
}

static inline int32_t memtable_node16_get_next_valid_child_idx(
    IN art_node* node,
    IN int32_t idx,
    OUT uint32_t *key_idx)
{
    int32_t next_idx = idx + 1;
    if (next_idx >= node->num_children)
    {
        return MAX_CHILDREN_NUM;
    }
    *key_idx = next_idx;
    return next_idx;
}

static inline int32_t memtable_node48_get_next_valid_child_idx(
    IN art_node* memtable_node,
    IN int32_t idx,
    OUT uint32_t *key_idx)
{
    int32_t next_idx = idx + 1;
    memtable_art_node48* node = (memtable_art_node48*)memtable_node;
    while (next_idx < MAX_CHILDREN_NUM && !node->keys[next_idx])
    {
        next_idx++;
    }
    *key_idx = next_idx;
    return next_idx == MAX_CHILDREN_NUM ? MAX_CHILDREN_NUM : (node->keys[next_idx] - 1);
}

static inline int32_t memtable_node256_get_next_valid_child_idx(
    IN art_node* node,
    IN int32_t idx,
    IN uint32_t *key_idx)
{
    int32_t next_idx = idx + 1;
     while (next_idx < MAX_CHILDREN_NUM && !check_art_memtable_node256_child_vaild(node, next_idx))
     {
         next_idx++;
     }
     *key_idx = next_idx;
     return next_idx;
}


int32_t memtable_node_get_next_valid_child_idx(
    IN art_node* node, 
    IN int32_t idx,
    IN uint32_t *key_idx)
{
    int32_t prev_idx = MAX_CHILDREN_NUM;
    switch(node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            prev_idx = memtable_node4_get_next_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE16:
            prev_idx = memtable_node16_get_next_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE48:
            prev_idx = memtable_node48_get_next_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE256:
            prev_idx = memtable_node256_get_next_valid_child_idx(node, idx, key_idx);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);
            break;
    }

    return prev_idx;
}

#endif

#if DESC("memtable get pre valid child idx")

static inline int32_t memtable_node4_get_prev_valid_child_idx(
    IN art_node* node,
    IN int32_t idx, 
    OUT uint32_t *key_idx)
{
    int32_t prev_idx = idx - 1;
    if (idx <= 0)
    {
        return MAX_CHILDREN_NUM;
    }
    if (MAX_CHILDREN_NUM == idx)
    {
        *key_idx = node->num_children - 1;
        return node->num_children - 1;
    }
    *key_idx = prev_idx;
    return prev_idx;
}

static inline int32_t memtable_node16_get_prev_valid_child_idx(
    IN art_node* node,
    IN int32_t idx,
    OUT uint32_t *key_idx)
{
    int32_t prev_idx = idx - 1;
    if (idx <= 0)
    {
        return MAX_CHILDREN_NUM;
    }
    if (MAX_CHILDREN_NUM == idx)
    {
        *key_idx = node->num_children - 1;
        return node->num_children - 1;
    }
    *key_idx = prev_idx;
    return prev_idx;
}

static inline int32_t memtable_node48_get_prev_valid_child_idx(
    IN art_node* memtable_node,
    IN int32_t idx,
    IN uint32_t *key_idx)
{
    memtable_art_node48* node = (memtable_art_node48*)memtable_node;
    int32_t prev_idx = idx - 1;
    if (idx <= 0)
    {
        return MAX_CHILDREN_NUM;
    }
    while (prev_idx >= 0 && !node->keys[prev_idx])
    {
        prev_idx--;
    }
    *key_idx = prev_idx;
    /*Using Unsigned integer flipping*/
    return (prev_idx < 0) ? MAX_CHILDREN_NUM : node->keys[prev_idx] - 1;
}


static inline int32_t memtable_node256_get_prev_valid_child_idx(
    IN art_node* node, 
    IN int32_t idx,
    IN uint32_t *key_idx)
{
    int32_t prev_idx = idx - 1;
    if (idx <= 0)
    {
        return MAX_CHILDREN_NUM;
    }
    while (prev_idx >= 0 && !check_art_memtable_node256_child_vaild(node, prev_idx))
    {
        prev_idx--;
    }
    *key_idx = prev_idx;
    return (prev_idx < 0) ? MAX_CHILDREN_NUM : prev_idx;
}

int32_t memtable_node_get_prev_valid_child_idx(
    IN art_node* node, 
    IN int32_t idx,
    IN uint32_t *key_idx)
{
    int32_t prev_idx = MAX_CHILDREN_NUM;
    switch(node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            prev_idx = memtable_node4_get_prev_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE16:
            prev_idx = memtable_node16_get_prev_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE48:
            prev_idx = memtable_node48_get_prev_valid_child_idx(node, idx, key_idx);
            break;
            
        case MEMTABLE_ART_NODE256:
            prev_idx = memtable_node256_get_prev_valid_child_idx(node, idx, key_idx);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);
            break;
    }

    return prev_idx;
}

#endif

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
    OUT uint32_t*  idx_in_parent)
{
    uint32_t child_idx   = 0;
    art_node** end_child = NULL;
	int32_t ret			 = kOk;

    if (parent_node->end_valid)
    {
        end_child = (art_node**)memtale_get_end_child(parent_node);
        *child = *end_child;
        *idx_in_parent = END_POSITION_INDEX;
        return kOk;
    }
    
    if (parent_node->num_children > 0)
    {
        child_idx = memtable_node_get_next_valid_child_idx(parent_node, -1, idx_in_parent);
        if (MAX_CHILDREN_NUM <= child_idx)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get left child node fail, the child_idx[%d] is more than MAX_CHILDREN_NUM[%d]",
                                    child_idx, MAX_CHILDREN_NUM);           
            return kNotFound;
        }
        ret = get_art_child_addr_by_index(parent_node, child_idx, (void**)child, NULL, NULL);
		if (ret != kOk)
		{
			ART_LOGERROR(ART_LOG_ID_4B00, "memtable_get_node_left_child get child fail, result[%d].", ret);
			return ret;
		}
        return kOk;
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get left child fail, parent_node->num_children[%d] is less than zero", parent_node->num_children);
    return kNotFound;
}

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
    OUT uint32_t*  idx_in_parent)
{
    uint32_t   child_idx = 0;
    art_node** end_child = NULL;
	int32_t ret			 = kOk;

    if (parent_node->num_children > 0)
    {
        child_idx = memtable_node_get_prev_valid_child_idx(parent_node, MAX_CHILDREN_NUM, idx_in_parent);
        if (MAX_CHILDREN_NUM == child_idx)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get right child node fail, the child_idx[%d] is equal with MAX_CHILDREN_NUM[%d]",
                                    child_idx, MAX_CHILDREN_NUM);             
            return kNotFound;
        }
        ret = get_art_child_addr_by_index(parent_node, child_idx, (void**)child, NULL, NULL);
		if (ret != kOk)
		{
			ART_LOGERROR(ART_LOG_ID_4B00, "memtable_get_node_right_child get child fail, result[%d].", ret);
			return ret;
		}
        return kOk;
    }
    /*Normally, shouldn't go here*/
    if (parent_node->end_valid)
    {
        end_child = (art_node**)memtale_get_end_child(parent_node);
        *child = *end_child;
        *idx_in_parent = END_POSITION_INDEX;
        return kOk;
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get right child fail, parent_node->num_children[%d] is less than zero", parent_node->num_children);    
    return kNotFound;
}

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
    OUT uint32_t*  idx_in_parent)
{
    int32_t  idx       = 0;
    uint32_t child_idx = 0;
	int32_t ret		   = kOk;

    if ((MAX_CHILDREN_NUM - 1) == start_idx || 0 == parent_node->num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get next child node fail, the child_num[%d] is zero or the next child(start_idx[%d]) is not existed.",
                                parent_node->num_children, start_idx);         
        return kNotFound;
    }
    
    idx = (int32_t)start_idx;
    if (END_POSITION_INDEX == start_idx)
    {
        idx = -1;
    }
    
    child_idx = memtable_node_get_next_valid_child_idx(parent_node, idx, idx_in_parent);
    if (MAX_CHILDREN_NUM == child_idx)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get next child node fail, the child_idx[%d] is equal with MAX_CHILDREN_NUM[%d]",
                                child_idx, MAX_CHILDREN_NUM);         
        return kNotFound;
    }
    ret = get_art_child_addr_by_index(parent_node, child_idx, (void**)child, NULL, NULL);
	if (ret != kOk)
	{
		ART_LOGERROR(ART_LOG_ID_4B00, "memtable_get_node_right_child get child fail, result[%d].", ret);
		return ret;
	}

    return kOk;
}

/**
 * get prev child of the node
 * @arg parent_node parent node
 * @arg start_idx the current child index
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @arg key_idx in node4 node16 node256, key_idx = idx_in_parent
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_get_node_prev_child(
    IN  art_node*  parent_node,
    IN  uint32_t   start_idx,
    OUT art_node** child,
    OUT uint32_t*  idx_in_parent)
{
     uint32_t   child_idx = 0;
     art_node** end_child = NULL;
	 uint32_t ret		  = kOk;

     /*已经是end child了*/
     if (END_POSITION_INDEX == start_idx)
     {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get pre child node fail, the start_idx[%d] is equal with END_POSITION_INDEX",
                                start_idx, END_POSITION_INDEX);        
         return kNotFound;
     }

     child_idx = memtable_node_get_prev_valid_child_idx(parent_node, start_idx, idx_in_parent);
     if (MAX_CHILDREN_NUM == child_idx)
     {
        if (parent_node->end_valid)
        {
            end_child = (art_node**)memtale_get_end_child(parent_node);
            *child = *end_child;
            *idx_in_parent = END_POSITION_INDEX;
            return kOk;
        }
        
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get pre valid child_idx[%d] is equal with MAX_CHILDREN_NUM and the end_valid[%d] of the node is false",
                            child_idx, parent_node->end_valid);          
        return kNotFound;
     }
     ret = get_art_child_addr_by_index(parent_node, child_idx, (void**)child, NULL, NULL);
	if (ret != kOk)
	{
		ART_LOGERROR(ART_LOG_ID_4B00, "memtable_get_node_right_child get child fail, result[%d].", ret);
		return ret;
	}
     return kOk;
}

#ifdef __cplusplus
}
#endif

