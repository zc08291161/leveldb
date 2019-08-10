#ifndef ART_NODE_TYPE_H_
#define ART_NODE_TYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

/*
*node type
*usrkey use art_node4/16/48/256
*snapid use art_snap_node4/16/48/256
*leaf use art_leaf, shared between memtable and sstable.
*/
// TODO:请勿随意改动枚举值，会导致校验type出错
typedef enum
{
    MEMTABLE_ART_NODE4              = 0,
    MEMTABLE_ART_NODE16             = 1,
    MEMTABLE_ART_NODE48             = 2,
    MEMTABLE_ART_NODE256            = 3,
    MEMTABLE_ART_SNAPNODE4          = 4,
    MEMTABLE_ART_SNAPNODE16         = 5,
    MEMTABLE_ART_SNAPNODE48         = 6,
    MEMTABLE_ART_SNAPNODE256        = 7,
    SSTABLE_ART_NODE4               = 8,
    SSTABLE_ART_NODE16              = 9,
    SSTABLE_ART_NODE48              = 10,
    SSTABLE_ART_NODE256             = 11,
    SSTABLE_ART_SNAPNODE4           = 12,
    SSTABLE_ART_SNAPNODE16          = 13,
    SSTABLE_ART_SNAPNODE48          = 14,
    SSTABLE_ART_SNAPNODE256         = 15,
    MEMTABLE_ART_LEAF               = 16,

    SSTABLE_ART_FUSION_NODE1        = 17,
    SSTABLE_ART_FUSION_SNAP_NODE1   = 18,
    SSTABLE_ART_FUSION_NODE4        = 19,
    SSTABLE_ART_FUSION_NODE16       = 20,
    SSTABLE_ART_FUSION_NODE256      = 21,
    /*serialize node type*/
    SERIALIZE_ART_PATH_NODE         = 22,
    /*merging node type*/
    MERGE_ART_PATH_NODE256          = 23,
    MERGE_ART_SNAP_PATH_NODE256     = 24,
    //pad type
    ART_FLUSH_PAD_BUFF              = 25,
    ART_MAGIC_FOR_8K                = 26,
    ART_NODE_BUTT
} ART_NODE_TYPE;

#ifdef __cplusplus
}
#endif
#endif

