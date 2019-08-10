#include <string.h>
#ifndef WIN32_DEBUG
//#include <stdbool.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#ifndef _SCM_PCLINT_
#include "art_util.h"
#include "art_log.h"
#include "art_snapshot_memtable.h"
#include "art_epoch.h"
#endif

//MODULE_ID(PID_ART);

#ifdef __cplusplus
extern "C" {
#endif

void snap_copy_header(IN art_snap_node* dest, IN art_snap_node* src)
{
    dest->num_children  = src->num_children;
    dest->partial_len   = src->partial_len;
    ART_MEMCPY(dest->partial, src->partial, src->partial_len);
}

static art_snap_node** add_snap_child256_in_new_node(
    IN memtable_art_snap_node256* node,
    IN uint8_t c,
    IN void* child)
{
    node->n.num_children++;
    node->children[c].mem_addr = (void*)child;
    return (art_snap_node**) & (node->children[c].mem_addr);
}

static art_snap_node** add_snap_child48_in_new_node(
    IN memtable_art_snap_node48* node,
    IN uint8_t c,
    IN void* child)
{
    uint8_t pos = 0;

    assert(node->n.num_children < 48);

    while (node->children[pos].mem_addr)
    {
        pos++;
    }

    node->children[pos].mem_addr = (void*)child;
    node->keys[c] = pos + 1;
    node->n.num_children++;
    return (art_snap_node**) & (node->children[pos].mem_addr);
}/*lint !e715*/

static art_snap_node** add_snap_child16_in_new_node(
    IN memtable_art_snap_node16* node,
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

        bitfield = _mm_movemask_epi8(result) & mask; /*lint !e718 !e746 !e737*/
#else 
        // Compare the key to all 16 stored keys
        for (uint32_t i = 0; i < 16; ++i)
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
            idx = __builtin_ctz(bitfield); /*lint !e718 !e746 !e732*/
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
        return (art_snap_node**) & (node->children[idx].mem_addr);
    }
} /*lint !e715*/

art_snap_node** add_snap_child4_in_new_node(
    IN art_node* snap_node4,
    IN uint8_t c,
    IN void* child)
{
    uint32_t idx = 0;
    memtable_art_snap_node4* node = (memtable_art_snap_node4*)snap_node4;

    assert(node->n.num_children < 4);

    for (idx = 0; idx < node->n.num_children; idx++)
    {
        if (c < node->keys[idx]) 
        {
            break;
        }
    }
    /*if idx == num_children, no need to make room*/
    if(idx != node->n.num_children)
    {
        // Shift to make room
        ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, (node->n.num_children - idx) * sizeof(uint8_t));
        ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx) * sizeof(memtable_art_child_t));
    }
    // Insert element
    node->keys[idx] = c;
    node->children[idx].mem_addr = (void*)child;
    node->n.num_children++;
    return (art_snap_node**) & (node->children[idx].mem_addr);
}/*lint !e715*/
/**
 * Add a snap child
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
art_snap_node** add_snap_child256(
    IN void* snap_node,
    IN uint8_t c,
    IN void* child)
{
    memtable_art_snap_node256* node = (memtable_art_snap_node256*)snap_node;

#ifndef SUPPORT_RW_CONCURRENCY
    
    node->n.num_children++;
    node->children[c].mem_addr = (void*)child;
    return (art_snap_node**) & (node->children[c].mem_addr);
    
#else
    node->n.num_children++;

    node->children[c].mem_addr = (void*)child;
    return (art_snap_node**) & (node->children[c].mem_addr);
#endif
}
/**
 * Add a snap child
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
art_snap_node** add_snap_child48(
    IN art_tree_t* t,
    IN void* snap_node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    int i                                   = 0;
    uint8_t pos                             = 0;
    memtable_art_snap_node48* node          = (memtable_art_snap_node48*)snap_node;
    memtable_art_snap_node256* new_node256  = NULL;
    art_snap_node** ret                     = NULL;

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
        return (art_snap_node**) & (node->children[pos].mem_addr);
#else
        node->children[pos].mem_addr = (void*)child;

        ART_MEMORY_BARRIER(); /*lint !e746 !e718*/ // node48:the keys must be changed after children(for search)

        node->keys[c] = pos + 1;
        node->n.num_children++;
        return (art_snap_node**) & (node->children[pos].mem_addr);     
#endif
    }
    else
    {
        new_node256 = (memtable_art_snap_node256*)alloc_node(t, MEMTABLE_ART_SNAPNODE256, arena_alloc_callback, args); /*lint !e740 !e826*/
        if (NULL == new_node256)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node256 fail when add child48 node.");
            return NULL;
        }

        for (i = 0; i < 256; i++)
        {
            if (node->keys[i])
            {
                new_node256->children[i] = node->children[node->keys[i] - 1];
            }
        }

        snap_copy_header((art_snap_node*)new_node256, (art_snap_node*)node);
#ifndef SUPPORT_RW_CONCURRENCY        
        *ref = (art_snap_node*)new_node256;
        reset_free_node(t, (art_node*)node);
        return add_snap_child256_in_new_node(new_node256, c, child);
#else
        ret = add_snap_child256_in_new_node(new_node256, c, child);

        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node256);

        art_mem_reclamation(t, (art_node*)node); /*lint !e740*/
        return ret;
#endif
    }
}

static art_snap_node** add_snap_child16_cow(
    IN unsigned bitfield, 
    IN art_tree_t* t,
    IN memtable_art_snap_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    unsigned idx;
    memtable_art_snap_node16* new_node16 = NULL;
    //  need to move children,cow
    idx = __builtin_ctz(bitfield); /*lint !e732*/

    new_node16 = (memtable_art_snap_node16*)alloc_node(t, MEMTABLE_ART_SNAPNODE16, arena_alloc_callback, args); /*lint !e740 !e826*/
    if (new_node16  == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node16 fail when add child node.");
        return NULL;
    }

    ART_MEMCPY(new_node16->children, node->children, sizeof(memtable_art_child_t)*idx);
    ART_MEMMOVE(new_node16->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
    ART_MEMCPY(new_node16->keys, node->keys, sizeof(uint8_t)*idx);
    ART_MEMMOVE(new_node16->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));
    

    snap_copy_header((art_snap_node*)new_node16, (art_snap_node*)node);

    // Set the child
    new_node16->keys[idx] = c;
    new_node16->children[idx].mem_addr = (void*)child;
    new_node16->n.num_children++;

    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node16);

    art_mem_reclamation(t, (art_node*)(void*)node);
    
    return (art_snap_node**) & (new_node16->children[idx].mem_addr);
}

static art_snap_node** add_snap_child16_to_child48(
    IN art_tree_t* t,
    IN memtable_art_snap_node16* node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    int i                                = 0;
    memtable_art_snap_node48* new_node48 = NULL;
    art_snap_node** ret                  = NULL;
    
    new_node48 = (memtable_art_snap_node48*)alloc_node(t, MEMTABLE_ART_SNAPNODE48, arena_alloc_callback, args); /*lint !e740 !e826*/
    if (new_node48  == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node48 fail when add child node.");
        return NULL;
    }
    // Copy the child pointers and populate the key map
    ART_MEMCPY(new_node48->children, node->children, sizeof(memtable_art_child_t)*node->n.num_children);
    for (i = 0; i < node->n.num_children; i++)
    {
        new_node48->keys[node->keys[i]] = i + 1;
    }

    snap_copy_header((art_snap_node*)new_node48, (art_snap_node*)node);
        
#ifndef SUPPORT_RW_CONCURRENCY
    *ref = (art_snap_node*)new_node48;
    reset_free_node(t, (art_node*)node);
    return add_snap_child48_in_new_node(new_node48, c, child);
#else
    ret = add_snap_child48_in_new_node(new_node48, c, child);

    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node48);

    art_mem_reclamation(t, (art_node*)(void*)node);
    return ret;
#endif
    
}
/**
 * Add a snap child
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
art_snap_node** add_snap_child16(
    IN art_tree_t* t,
    IN void* snap_node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    unsigned mask;
    unsigned idx;    
    unsigned bitfield              = 0;
    memtable_art_snap_node16* node = (memtable_art_snap_node16*)snap_node;
    
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
        for (uint8_t i = 0; i < 16; ++i)
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
        
        __sync_synchronize();// node16:the num_children must be changed after key and children(for search)

        node->n.num_children++;
        return (art_snap_node**) & (node->children[idx].mem_addr);
#else
        if (bitfield)
        {
            return add_snap_child16_cow(bitfield, t, node, ref, c, child, arena_alloc_callback, args);
        }
        else
        { 
            idx = node->n.num_children; 

            // Set the child
            node->keys[idx] = c;
            node->children[idx].mem_addr = (void*)child;
            
            ART_MEMORY_BARRIER();// node16:the num_children must be changed after key and children(for search)

            node->n.num_children++;
            return (art_snap_node**) & (node->children[idx].mem_addr);
        }     
#endif

    }
    else
    {
        return add_snap_child16_to_child48(t, node, ref, c, child, arena_alloc_callback, args);
    }
}
/**
 * Add a snap child
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
art_snap_node** add_snap_child4(
    IN art_tree_t* t,
    IN void* snap_node,
    IN void** ref,
    IN uint8_t c,
    IN void* child,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    memtable_art_snap_node4* node        = (memtable_art_snap_node4*)snap_node;
    memtable_art_snap_node16* new_node16 = NULL;
    memtable_art_snap_node4* new_node4   = NULL;
    uint32_t idx                         = 0;
    art_snap_node** ret                  = NULL;

    if (node->n.num_children < 4)
    {

        for (; idx < node->n.num_children; idx++)
        {
            if (c < node->keys[idx])
            {
                break;
            }
        }
        
#ifndef SUPPORT_RW_CONCURRENCY

        // Shift to make room
        ART_MEMMOVE(node->keys + idx + 1, node->keys + idx, node->n.num_children - idx);
        ART_MEMMOVE(node->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
        // Insert element
        node->keys[idx] = c;
        node->children[idx].mem_addr = (void*)child;
        node->n.num_children++;
        return (art_snap_node**) & (node->children[idx].mem_addr);
#else
        if (idx == node->n.num_children)
        {
        
            // if no need move key and children, directly insert,no cow
            node->keys[idx] = c;
            node->children[idx].mem_addr = (void*)child;
            
            ART_MEMORY_BARRIER();// node4:the num_children must be changed after key and children(for search)
            
            node->n.num_children++;
            return (art_snap_node**) & (node->children[idx].mem_addr);

        }else
        {
            new_node4 = (memtable_art_snap_node4*)alloc_node(t, MEMTABLE_ART_SNAPNODE4, arena_alloc_callback, args); /*lint !e740 !e826*/
            if (new_node4  == NULL)
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node4 fail when add child node.");
                return NULL;
            }
            
            ART_MEMCPY(new_node4->children, node->children, sizeof(memtable_art_child_t)*idx);
            ART_MEMCPY(new_node4->children + idx + 1, node->children + idx, (node->n.num_children - idx)*sizeof(memtable_art_child_t));
            
            ART_MEMCPY(new_node4->keys, node->keys, sizeof(uint8_t)*idx);
            ART_MEMCPY(new_node4->keys + idx + 1, node->keys + idx, (node->n.num_children - idx)*sizeof(uint8_t));
            
            snap_copy_header((art_snap_node*)new_node4, (art_snap_node*)node);
            new_node4->keys[idx] = c;
            new_node4->children[idx].mem_addr = (void*)child;
            new_node4->n.num_children++;
            
            ART_MEMORY_BARRIER();
            ART_ATOMIC_SET_PTR(ref, new_node4);
            
            art_mem_reclamation(t, (art_node*)(void*)node);
            return (art_snap_node**) & (new_node4->children[idx].mem_addr);

        }
#endif

    }
    else
    {
        new_node16 = (memtable_art_snap_node16*)alloc_node(t, MEMTABLE_ART_SNAPNODE16, arena_alloc_callback, args); /*lint !e740 !e826*/
        if (new_node16  == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node16 fail when add child node.");
            return NULL;
        }
        // Copy the child pointers and the key map
        ART_MEMCPY(new_node16->children, node->children, sizeof(memtable_art_child_t)*node->n.num_children);
        ART_MEMCPY(new_node16->keys, node->keys, sizeof(uint8_t)*node->n.num_children);
        snap_copy_header((art_snap_node*)new_node16, (art_snap_node*)node);
#ifndef SUPPORT_RW_CONCURRENCY

        *ref = (art_snap_node*)new_node16;
        reset_free_node(t, (art_node*)node);
        return add_snap_child16_in_new_node(new_node16, c, child);
#else
        ret = add_snap_child16_in_new_node(new_node16, c, child);

        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node16);

        art_mem_reclamation(t, (art_node*)(void*)node);
        return ret;
#endif

    }
}


art_snap_node** memtable_snap_find_child(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN uint8_t c,
    OUT int* child_idx)
{
    int32_t  i        = 0;
    uint32_t mask     = 0;
    uint32_t bitfield = 0;
    union
    {
        memtable_art_snap_node4* p1;
        memtable_art_snap_node16* p2;
        memtable_art_snap_node48* p3;
        memtable_art_snap_node256* p4;
    } p;

    switch (node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            p.p1 = (memtable_art_snap_node4*)node;

            for (i = 0 ; i < node->num_children; i++)
            {
                /* this cast works around a bug in gcc 5.1 when unrolling loops
                 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
                 */
                if ((p.p1->keys)[i] == c)
                {
                    *child_idx = i;
                    return (art_snap_node**)&(p.p1->children[i].mem_addr);
                }
            }

            break;

        case MEMTABLE_ART_SNAPNODE16:
            p.p2 = (memtable_art_snap_node16*)node;

            // support non-86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION
            // Compare the key to all 16 stored keys
            __m128i cmp;
            cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), /*lint !e718 !e746*/
                                 _mm_loadu_si128((__m128i*)p.p2->keys)); /*lint !e26 !e63 !e718 !e746*/

            // Use a mask to ignore children that don't exist
            mask = (1 << node->num_children) - 1;
            bitfield = _mm_movemask_epi8(cmp) & mask;
#else
            // Compare the key to all 16 stored keys
            bitfield = 0;

            for (i = 0; i < 16; ++i)
            {
                if (p.p2->keys[i] == c)
                {
                    bitfield |= (1 << i);
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
                *child_idx = __builtin_ctz(bitfield); /*lint !e516*/
                return (art_snap_node**)&(p.p2->children[__builtin_ctz(bitfield)].mem_addr);
            }
            break;


        case MEMTABLE_ART_SNAPNODE48:
            p.p3 = (memtable_art_snap_node48*)node;
            i = p.p3->keys[c];

            if (i)
            {                
                *child_idx = c;
                return (art_snap_node**)&(p.p3->children[i-1].mem_addr);
            }
            break;

        case MEMTABLE_ART_SNAPNODE256:
            p.p4 = (memtable_art_snap_node256*)node;

            if (check_art_snap_node256_valid(t, node, c))

                //if (p.p4->children[c].mem_addr)
            {        
                *child_idx = c;
                return (art_snap_node**)&(p.p4->children[c].mem_addr);
            }
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);             
            ART_ASSERT(0);
            break;
    }

    return NULL;
}

int32_t memtable_snap_get_node4_left_child(
    IN void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node4* parent_node = (memtable_art_snap_node4*)node;
        
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get left node in node4[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[0].mem_addr;
    *idx_in_parent = 0;
    
    return kOk;
}


int32_t memtable_snap_get_node16_left_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node16* parent_node = (memtable_art_snap_node16*)node;
        
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get left node in node16[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);        
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[0].mem_addr;
    *idx_in_parent = 0;
    
    return kOk;
}


int32_t memtable_snap_get_node48_left_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    uint32_t idx                          = 0;
    memtable_art_snap_node48* parent_node = (memtable_art_snap_node48*)node;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get left node in node48[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);        
        return kNotFound;
    }

    while(MAX_CHILDREN_NUM > idx && 0 == parent_node->keys[idx])
    {
        ++idx;
    }
    //issue 3208
    if (MAX_CHILDREN_NUM == idx)
    {
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[parent_node->keys[idx] - 1].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}


int32_t memtable_snap_get_node256_left_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    uint32_t idx                           = 0;
    memtable_art_snap_node256* parent_node = (memtable_art_snap_node256*)node;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get left node in node256[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);        
        return kNotFound;
    }

    while(MAX_CHILDREN_NUM > idx && NULL == parent_node->children[idx].mem_addr)
    {
        ++idx;
    }
    //issue 3209
    if (MAX_CHILDREN_NUM == idx)
    {
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[idx].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}

/**
 * get left child of the node
 * @art parent_node parent node
 * @arg child the child
 * @arg idx_in_parent the child's index in parent,
        END_POSITION_INDEX means end child
 * @return kOk means get successfully, others mean failed.
 */
int32_t memtable_snap_get_node_left_child(
    IN  art_snap_node* parent_node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    int32_t result              = kNotFound;
    
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_get_node4_left_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_get_node16_left_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_get_node48_left_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_get_node256_left_child((void*)parent_node, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            break;
    }

    return result;
}



int32_t memtable_snap_get_node4_right_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node4* parent_node = (memtable_art_snap_node4*)node;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get right node in node4[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);        
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[parent_node->n.num_children - 1].mem_addr;
    *idx_in_parent = (uint32_t)parent_node->n.num_children - 1;
    
    return kOk;
}


int32_t memtable_snap_get_node16_right_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node16* parent_node = (memtable_art_snap_node16*)node;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get right node in node16[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);         
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[parent_node->n.num_children - 1].mem_addr;
    *idx_in_parent = (uint32_t)parent_node->n.num_children - 1;
    
    return kOk;
}


int32_t memtable_snap_get_node48_right_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node48* parent_node = (memtable_art_snap_node48*)node;
    int32_t idx                           = MAX_CHILDREN_NUM - 1;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get right node in node48[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);         
        return kNotFound;
    }

    while(0 <= idx && 0 == parent_node->keys[idx])
    {
        --idx;
    }
    
    *child = (art_snap_node*)parent_node->children[parent_node->keys[idx] - 1].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}


int32_t memtable_snap_get_node256_right_child(
    IN  void* node,    
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    uint32_t idx                           = MAX_CHILDREN_NUM - 1;
    memtable_art_snap_node256* parent_node = (memtable_art_snap_node256*)node;
    
    if(0 == parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get right node in node256[%p] fail, the child_num[%d] is zero.",
                                parent_node, parent_node->n.num_children);         
        return kNotFound;
    }

    while(0 <= idx && NULL == parent_node->children[idx].mem_addr)
    {
        --idx;
    }
    
    *child = (art_snap_node*)parent_node->children[idx].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}

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
    OUT uint32_t* idx_in_parent)
{
    int32_t result              = kNotFound;
    
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_get_node4_right_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_get_node16_right_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_get_node48_right_child((void*)parent_node, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_get_node256_right_child((void*)parent_node, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            break;
    }

    return result;

}


int32_t memtable_snap_get_node4_next_child(
    IN void* node, 
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node4* parent_node = (memtable_art_snap_node4*)node;
    
    if(start_idx + 1 >= parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get next node in node4[%p] fail, the child_num[%d] is less than or equal to start_idx[%d] + 1.",
                                parent_node, parent_node->n.num_children, start_idx);         
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[start_idx + 1].mem_addr;
    *idx_in_parent = start_idx + 1;
    
    return kOk;
}


int32_t memtable_snap_get_node16_next_child(
    IN void* node, 
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node16* parent_node = (memtable_art_snap_node16*)node;

    if(start_idx + 1 >= parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get next node in node16[%p] fail, the child_num[%d] is less than or equal to start_idx[%d] + 1.",
                                parent_node, parent_node->n.num_children, start_idx);         
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[start_idx + 1].mem_addr;
    *idx_in_parent = start_idx + 1;
    
    return kOk;
}


int32_t memtable_snap_get_node48_next_child(
    IN void* node,  
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    uint32_t idx                          = start_idx + 1;
    memtable_art_snap_node48* parent_node = (memtable_art_snap_node48*)node;
    
    while(MAX_CHILDREN_NUM > idx && 0 == parent_node->keys[idx])
    {
        ++idx;
    }

    if(MAX_CHILDREN_NUM <= idx)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get next node in node48[%p] fail, the idx[%d] is more than MAX_CHILDREN_NUM.",
                                parent_node, idx);         
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[parent_node->keys[idx] - 1].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}


int32_t memtable_snap_get_node256_next_child(
    IN void* node,   
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    uint32_t idx                           = start_idx + 1;
    memtable_art_snap_node256* parent_node = (memtable_art_snap_node256*)node;
    
    while(MAX_CHILDREN_NUM > idx && NULL == parent_node->children[idx].mem_addr)
    {
        ++idx;
    }

    if(MAX_CHILDREN_NUM <= idx)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get next node in node256[%p] fail, the idx[%d] is more than MAX_CHILDREN_NUM.",
                                parent_node, idx);          
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[idx].mem_addr;
    *idx_in_parent = idx;
    
    return kOk;
}

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
    OUT uint32_t* idx_in_parent)
{
    int32_t result              = kNotFound;
    
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_get_node4_next_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_get_node16_next_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_get_node48_next_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_get_node256_next_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            break;
    }

    return result;
}


int32_t memtable_snap_get_node4_prev_child(
    IN void* node, 
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node4* parent_node = (memtable_art_snap_node4*)node;
    
    if(0 == start_idx || start_idx + 1 > parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get prev node in node4[%p] fail, start_idx[%d] child_num[%d].",
                                parent_node, start_idx, parent_node->n.num_children);          
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[start_idx - 1].mem_addr;
    *idx_in_parent = start_idx - 1;
    
    return kOk;
}


int32_t memtable_snap_get_node16_prev_child(
    IN void* node, 
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    memtable_art_snap_node16* parent_node = (memtable_art_snap_node16*)node;
    
    if(0 == start_idx || start_idx + 1 > parent_node->n.num_children)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get prev node in node16[%p] fail, start_idx[%d] child_num[%d].",
                                parent_node, start_idx, parent_node->n.num_children);         
        return kNotFound;
    }

    *child = (art_snap_node*)parent_node->children[start_idx - 1].mem_addr;
    *idx_in_parent = start_idx - 1;
    
    return kOk;
}


int32_t memtable_snap_get_node48_prev_child(
    IN void* node,  
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    int32_t idx                           = (int32_t)start_idx - 1;
    memtable_art_snap_node48* parent_node = (memtable_art_snap_node48*)node;
    
    while(0 <= idx && 0 == parent_node->keys[idx])
    {
        --idx;
    }

    if(0 > idx)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get prev node in node48[%p] fail, idx[%d].",
                                parent_node, idx);         
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[parent_node->keys[idx] - 1].mem_addr;
    *idx_in_parent = (uint32_t)idx;
    
    return kOk;
}


int32_t memtable_snap_get_node256_prev_child(
    IN void* node,   
    IN uint32_t start_idx, // prev child index
    OUT art_snap_node** child,
    OUT uint32_t* idx_in_parent)
{
    int32_t idx                            = (int32_t)start_idx - 1;
    memtable_art_snap_node256* parent_node = (memtable_art_snap_node256*)node;
    
    while(0 <= idx && NULL == parent_node->children[idx].mem_addr)
    {
        --idx;
    }

    if(0 > idx)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap get prev node in node256[%p] fail, idx[%d].",
                                parent_node, idx);        
        return kNotFound;
    }
    
    *child = (art_snap_node*)parent_node->children[idx].mem_addr;
    *idx_in_parent = (uint32_t)idx;
    
    return kOk;
}

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
    OUT uint32_t* idx_in_parent)
{
    int32_t result              = kNotFound;
    
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_get_node4_prev_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_get_node16_prev_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_get_node48_prev_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_get_node256_prev_child((void*)parent_node, start_idx, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            break;
    }

    return result;
}


static int32_t memtable_snap_find_node4_child(
    IN art_snap_node* snap_node,
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                         = 0;
    memtable_art_snap_node4* node4 = (memtable_art_snap_node4*)snap_node;
    
    for (i = 0; i < snap_node->num_children; i++)
    {
        if (node4->keys[i] == c)
        {
            *idx_in_parent = i;
            *child = (art_snap_node*)node4->children[i].mem_addr;
            return NULL == *child ? kArtSnapIdNotFound : kOk;
        }
        if (node4->keys[i] > c)
        {
            break;
        }
    }

    return kArtSnapIdNotFound;
}


static int32_t memtable_snap_find_node16_child(
    IN art_snap_node* snap_node,
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int      i        = 0;
    uint32_t mask     = 0;
    uint32_t bitfield = 0;
    memtable_art_snap_node16* node16 = (memtable_art_snap_node16*)snap_node;

    // support non-86 architectures
#ifdef NODE16_USE_MACHINE_INSTRUCTION
    // Compare the key to all 16 stored keys
    __m128i cmp;
    cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), /*lint !e718 !e746*/
        _mm_loadu_si128((__m128i*)node16->keys)); /*lint !e26 !e63 !e718 !e746*/

                                                // Use a mask to ignore children that don't exist
    mask = (1 << node->num_children) - 1;
    bitfield = _mm_movemask_epi8(cmp) & mask;
#else
    // Compare the key to all 16 stored keys
    bitfield = 0;

    for (i = 0; i < snap_node->num_children; ++i)
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
    mask = (1 << snap_node->num_children) - 1;
    bitfield &= mask;
#endif

    /*
    * If we have a match (any bit set) then we can
    * return the pointer match using ctz to get
    * the index.
    */
    if (bitfield)
    {
        *idx_in_parent = __builtin_ctz(bitfield); /*lint !e516*/
        *child =  (art_snap_node*)node16->children[__builtin_ctz(bitfield)].mem_addr;
        return NULL == *child ? kArtSnapIdNotFound : kOk;
    }
    return kArtSnapIdNotFound;
}


static int32_t memtable_snap_find_node48_child(
    IN art_snap_node* snap_node,
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                          = 0;
    memtable_art_snap_node48* node48 = (memtable_art_snap_node48*)snap_node;

    i = node48->keys[c];
    if (i && NULL != node48->children[i - 1].mem_addr)
    {
        *idx_in_parent = c;
        *child = (art_snap_node*)node48->children[i - 1].mem_addr;
        return kOk;
    }

    return kArtSnapIdNotFound;
}


static int32_t memtable_snap_find_node256_child(
    IN art_snap_node* snap_node,
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                              = c;
    memtable_art_snap_node256* node256 = (memtable_art_snap_node256*)snap_node;

    if(NULL != node256->children[c].mem_addr)
    {
        *child = (art_snap_node *)node256->children[i].mem_addr;
        *idx_in_parent = c;
        return kOk;
    }

    return kArtSnapIdNotFound;
}


/*
* description               : batch search one key in memtable snap tree.
*/
int32_t memtable_snap_search_node_equal(
    art_snap_node*  parent_node,
    uint32_t snapid,
	uint32_t snapid_len,
	uint32_t depth,
    art_snap_node** child,
    uint32_t*  idx_in_parent)
{
    uint32_t same_len  = 0;
    uint32_t result    = kOk;
    uint8_t key        = 0;
    
    
    same_len = snap_check_prefix(parent_node, snapid, snapid_len, depth);
    depth += same_len;
    
    ART_ASSERT(depth != snapid_len);
    key = GET_CHAR_FROM_INT(snapid, (snapid_len - depth - 1));
    if(same_len != parent_node->partial_len)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "search snapid[%u] failed, partial length is [%u] longest same length is [%u].",
                     parent_node->partial_len, same_len);
        return kArtSnapIdNotFound;
    }

    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_find_node4_child(parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_find_node16_child(parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_find_node48_child(parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_find_node256_child(parent_node, key, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            ART_ASSERT(0);
            result = kIOError;
            break;
    }

    return result;
}


int32_t memtable_snap_find_node4_child_equal_or_bigger(
    IN void* snap_node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                         = 0;
    memtable_art_snap_node4* node = (memtable_art_snap_node4*)snap_node;
    
    while(i < node->n.num_children && node->keys[i] < c)
    {
        ++i;
    }


    if(i == node->n.num_children)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


int32_t memtable_snap_find_node16_child_equal_or_bigger(
    IN void* snap_node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                          = 0;
    memtable_art_snap_node16* node = (memtable_art_snap_node16*)snap_node;
    
    while(i < node->n.num_children && node->keys[i] < c)
    {
        ++i;
    }


    if(i == node->n.num_children)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


int32_t memtable_snap_find_node48_child_equal_or_bigger(
    IN void* snap_node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                          = c;
    memtable_art_snap_node48* node = (memtable_art_snap_node48*)snap_node;
    
    while(i < MAX_CHILDREN_NUM && 0 == node->keys[i])
    {
        ++i;
    }


    if(i == MAX_CHILDREN_NUM)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_snap_node *)node->children[node->keys[i] - 1].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[c] != 0)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_GREATER;
}


int32_t memtable_snap_find_node256_child_equal_or_bigger(
    IN void* snap_node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                           = c;
    memtable_art_snap_node256* node = (memtable_art_snap_node256*)snap_node;
    
    while(i < MAX_CHILDREN_NUM && NULL == node->children[i].mem_addr)
    {
        ++i;
    }


    if(i == MAX_CHILDREN_NUM)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
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
int32_t memtable_snap_search_node_equal_or_bigger(
    IN  art_snap_node*  parent_node,
    IN  uint32_t snapid,
	IN  uint32_t snapid_len,
	IN  uint32_t depth,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int same_len                            = 0;
    int result                              = kOk;
    uint8_t key                             = 0;
    
    if(!is_snap_inner_node(parent_node))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "memtable snap search the node is not snap inner node.");
        ART_ASSERT(0);
        return kIOError;
    }
    
    same_len = snap_check_prefix(parent_node, snapid, snapid_len, depth);
    depth += same_len;
    
    assert(depth != snapid_len);
    key = GET_CHAR_FROM_INT(snapid, (snapid_len - depth - 1));
    if(same_len != parent_node->partial_len && parent_node->partial[same_len] < key)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    if(same_len != parent_node->partial_len && parent_node->partial[same_len] > key)
    {
        result = memtable_snap_get_node_left_child(parent_node, child, idx_in_parent);
        assert(kOk == result);

        return ART_COMPARE_RESULT_GREATER;
    }

    // finally, same_len == parent_node->partial_len
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_find_node4_child_equal_or_bigger((void *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_find_node16_child_equal_or_bigger((void *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_find_node48_child_equal_or_bigger((void *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_find_node256_child_equal_or_bigger((void *)parent_node, key, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            ART_ASSERT(0);
            result = kIOError;
            break;
    }

    return result;
}


void* memtable_get_snap_child_addr_by_index(art_snap_node* node, int idx)
{
    void* child = NULL;

    union
    {
        memtable_art_snap_node4* p1;
        memtable_art_snap_node16* p2;
        memtable_art_snap_node48* p3;
        memtable_art_snap_node256* p4;
    }p;

    /* judge node_type to select node by index */
    switch (node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:            
            p.p1 = (memtable_art_snap_node4*)node;
            child = p.p1->children[idx].mem_addr;
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            p.p2 = (memtable_art_snap_node16*)node;
            child = p.p2->children[idx].mem_addr;
            break;
            
        case MEMTABLE_ART_SNAPNODE48:            
            p.p3 = (memtable_art_snap_node48*)node;
            child = p.p3->children[idx].mem_addr;
            break;

        case MEMTABLE_ART_SNAPNODE256:
            p.p4 = (memtable_art_snap_node256*)node;
            child = p.p4->children[idx].mem_addr;
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);              
            ART_ASSERT(0);
            break;
    }

    return child;
}

void destroy_snap_node256(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                              = 0;
    memtable_art_snap_node256* node256 = (memtable_art_snap_node256*)node;
    for (i = 0; i < 256; i++)
    {
        if (node256->children[i].mem_addr)
        { destroy_node(node256->children[i].mem_addr, arena_destroy_callback); }
    }
}

void destroy_snap_node48(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i;
    memtable_art_snap_node48* node48 = (memtable_art_snap_node48*)node;
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node48->children[i].mem_addr, arena_destroy_callback);
    }
}

void destroy_snap_node16(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i;
    memtable_art_snap_node16* node16 = (memtable_art_snap_node16*)node;
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node16->children[i].mem_addr, arena_destroy_callback);
    }
}

void destroy_snap_node4(
    IN art_snap_node* node,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    int i                          = 0;
    memtable_art_snap_node4* node4 = (memtable_art_snap_node4*)node;
    for (i = 0; i < node->num_children; i++)
    {
        destroy_node(node4->children[i].mem_addr, arena_destroy_callback);
    }
}

uint32_t get_memtable_snap_node_type_size(uint8_t node_type)
{
    uint32_t size = 0;
    
    switch (node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            size = sizeof(memtable_art_snap_node4);
            break;

        case MEMTABLE_ART_SNAPNODE16:
            size = sizeof(memtable_art_snap_node16);
            break;

        case MEMTABLE_ART_SNAPNODE48:
            size = sizeof(memtable_art_snap_node48);
            break;

        case MEMTABLE_ART_SNAPNODE256:
            size = sizeof(memtable_art_snap_node256);
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node_type);           
            ART_ASSERT(0);
            break;
    }

    return size;
}

/*uint8_t memtable_snap_node_get_key(art_node* node, int key_idx)
{
    union
    {
        memtable_art_snap_node4* p1;
        memtable_art_snap_node16* p2;
        memtable_art_snap_node48* p3;
        memtable_art_snap_node256* p4;
    }p;

    switch (node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:            
            p.p1 = (memtable_art_snap_node4*)node;
            return p.p1->keys[key_idx];
        
        case MEMTABLE_ART_SNAPNODE16:
            p.p2 = (memtable_art_snap_node16*)node;
            return p.p2->keys[key_idx];
        
        case MEMTABLE_ART_SNAPNODE48:            
            return (uint8_t)key_idx;
        
        case MEMTABLE_ART_SNAPNODE256:
            return (uint8_t)key_idx;

        default:
            ART_ASSERT(0);
    }
    
    return kOk;
            
}*/

bool is_memtable_snap_inner_node(IN art_snap_node* node)
{
    return ((node->node_type >= MEMTABLE_ART_SNAPNODE4) && (node->node_type <= MEMTABLE_ART_SNAPNODE256));
}

/**
 * get most right child of the snap root node
 * @arg snap_root leaf node or snap root node
 * @arg is_snap_root_leaf is snap root leaf node
 * @return leaf.
 */
void* memtable_snap_get_most_right(
    IN art_snap_node* snap_root,
    OUT bool *is_snap_root_leaf)
{
    int32_t result                      = kOk;
	art_snap_node *right_child          = NULL;
    uint32_t idx_in_parent              = 0;
	art_snap_node *parent_node          = snap_root;
    
    if(MEMTABLE_ART_LEAF == snap_root->node_type)
    {
        *is_snap_root_leaf = true;
        return (void*)snap_root;
    }
    
    while(MEMTABLE_ART_LEAF != parent_node->node_type)
    {
        result = memtable_snap_get_node_right_child(parent_node, &right_child, &idx_in_parent);
        parent_node = right_child;
    }

    *is_snap_root_leaf = false;
    
    //TODO
    /* 该值未使用，后期接口整改进行完善 */ 
    UNREFERENCE_PARAM(result);
    return (void*)right_child;
}
/*
int32_t memtable_snap_find_node4_child_equal_or_smaller(
    IN memtable_art_snap_node4* node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = node->n.num_children - 1;

    assert(0 < node->n.num_children);
    while(0 <= i && node->keys[i] > c)
    {
        --i;
    }


    if(0 > i)
    {
        return ART_COMPARE_RESULT_GREATER;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_LESS;
}


int32_t memtable_snap_find_node16_child_equal_or_smaller(
    IN memtable_art_snap_node16* node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = node->n.num_children - 1;

    assert(0 < node->n.num_children);
    while(0 <= i && node->keys[i] > c)
    {
        --i;
    }


    if(0 > i)
    {
        return ART_COMPARE_RESULT_GREATER;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[i] == c)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_LESS;
}


int32_t memtable_snap_find_node48_child_equal_or_smaller(
    IN memtable_art_snap_node48* node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = c;

    while(0 <= i && 0 == node->keys[i])
    {
        --i;
    }


    if(0 > i)
    {
        return ART_COMPARE_RESULT_GREATER;
    }

    *child = (art_snap_node *)node->children[node->keys[i] - 1].mem_addr;
    *idx_in_parent = i;
    
    if(node->keys[c] != 0)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_LESS;
}


int32_t memtable_snap_find_node256_child_equal_or_smaller(
    IN memtable_art_snap_node256* node, 
    IN uint8_t c,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int i                       = c;

    while(0 <= i && NULL == node->children[i].mem_addr)
    {
        --i;
    }


    if(0 > i)
    {
        return ART_COMPARE_RESULT_GREATER;
    }

    *child = (art_snap_node *)node->children[i].mem_addr;
    *idx_in_parent = i;
    
    if(NULL != node->children[c].mem_addr)
    {
        return ART_COMPARE_RESULT_EQUAL;
    }

    return ART_COMPARE_RESULT_LESS;
}*/


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
/*int32_t memtable_snap_search_node_equal_or_smaller(
    IN  art_snap_node*  parent_node,
    IN  uint32_t snapid,
	IN  uint32_t snapid_len,
	IN  uint32_t depth,
    OUT art_snap_node** child,
    OUT uint32_t*  idx_in_parent)
{
    int same_len                            = 0;
    int result                              = kOk;
    uint8_t key                             = 0;
    
    assert((is_snap_inner_node(parent_node)));// lint !e666
    
    same_len = snap_check_prefix(parent_node, snapid, snapid_len, depth);
    depth += same_len;
    
    assert(depth != snapid_len);
    key = GET_CHAR_FROM_INT(snapid, (snapid_len - depth - 1));
    if(same_len != parent_node->partial_len && parent_node->partial[same_len] < key)
    {
        result = memtable_snap_get_node_right_child(parent_node, child, idx_in_parent);
        assert(kOk == result);
        return ART_COMPARE_RESULT_LESS;
    }

    if(same_len != parent_node->partial_len && parent_node->partial[same_len] > key)
    {
        return ART_COMPARE_RESULT_GREATER;
    }

    // finally, same_len == parent_node->partial_len
    switch(parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            result = memtable_snap_find_node4_child_equal_or_smaller((memtable_art_snap_node4 *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            result = memtable_snap_find_node16_child_equal_or_smaller((memtable_art_snap_node16 *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE48:
            result = memtable_snap_find_node48_child_equal_or_smaller((memtable_art_snap_node48 *)parent_node, key, child, idx_in_parent);
            break;
            
        case MEMTABLE_ART_SNAPNODE256:
            result = memtable_snap_find_node256_child_equal_or_smaller((memtable_art_snap_node256 *)parent_node, key, child, idx_in_parent);
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", parent_node->node_type);
            ART_ASSERT(0);
            break;
    }

    return result;
}
*/
#ifdef __cplusplus
}
#endif

