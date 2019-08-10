#ifndef ART_MEMTABLE_SNAP_IO_H_
#define ART_MEMTABLE_SNAP_IO_H_

#ifdef __cplusplus
extern "C" {
#endif


/**
 * search value from the snap node
 * @arg t The tree
 * @arg parent_node snap root/leaf
 * @arg snapshotid
 * @return kOK means successfully, otherwise other code
 */
int32_t memtable_snap_search(
    IN art_tree_t* t,
    IN art_snap_node* parent_node,
    IN artree_get_para_t *get_para);

#ifdef __cplusplus
}
#endif
#endif
