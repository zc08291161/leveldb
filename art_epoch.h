#ifndef ART_EPOCH_H_
#define ART_EPOCH_H_

#ifdef __cplusplus
extern "C" {
#endif

void epoch_init(IN epoch_based_reclamation_t* epoch_manager);

/*  handle the freed node in write thread*/
void art_mem_reclamation(IN art_tree_t* t, IN art_node* node);
#ifdef __cplusplus
}
#endif

#endif
