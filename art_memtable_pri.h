#ifndef ART_MEMTABLE_PRIVATE_H_
#define ART_MEMTABLE_PRIVATE_H_

#ifdef __cplusplus
extern "C" {
#endif

/*
 * child addr for memtable,use union structure
 */
typedef union memtable_art_child
{
    void                        *mem_addr;
} memtable_art_child_t;


#ifdef __cplusplus
}
#endif
#endif

