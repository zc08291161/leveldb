#ifndef ART_PUB_H_
#define ART_PUB_H_


#include "art.h"
#include "art_util.h"
#include "securec.h"
#ifndef WIN32_DEBUG
#include "db_type.h"
#else
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifndef WIN32_DEBUG
/*用于将value的值写入ptr的位置，同时返回ptr之前存储的值*/
#define ART_ATOMIC_SET_PTR(child, new_addr)         (*child = new_addr)
#define ART_MEMORY_BARRIER(...)                     __sync_synchronize()
#else
#define ART_ATOMIC_SET_PTR(child, new_addr)         (*child = new_addr)
#define ART_MEMORY_BARRIER(...)                    
#endif


#define GET_CHAR_FROM_INT(snapid,idx)               (*((uint8_t*)&(snapid) + idx))

#define GET_EPOCH_HEAD(node,offset)                 (void*)(((char*)node - offset))
#define GET_ART_HEAD(node,offset)                   (void*)(((char*)node + offset))

#define GET_ARTREE_APPLY_TYPE(t)                    (((art_tree_t*)(t))->apply_type)
#define GET_ARTREE_KEY_VALUE_TYPE(t)                (((art_tree_t*)(t))->art_configs.key_value_type)
#define GET_ARTREE_FUSION_NODE_OPEN_STATE(t)        (((art_tree_t*)(t))->art_configs.is_fusion_node_open)

#define GET_ARTREE_NODE_TYPE(n)                     (((art_node*)n)->node_type)


#define IS_LUNMAP_ARTREE(t) (GET_ARTREE_FUSION_NODE_OPEN_STATE(t))

#define CHECK_ARTREE_CRC_VALID(t) ((((art_tree_t*)(t))->art_crc) == ART_ROOT_CRC)

#ifndef UNREFERENCE_PARAM
#define UNREFERENCE_PARAM(x)                ((void)(x))
#endif

#ifdef _SCM_PCLINT_
#define ART_ASSERT(x)
#else
#define ART_ASSERT(x) assert(x)
#endif

#define ART_MEMCPY(dest, src, count)        memcpy_s(dest, count, src, count)
#ifdef WIN32_DEBUG
#define MAX(a,b) (((a)>(b))?(a):(b))
//#define ART_MEMSET(dest, c, count)          memset(dest, c, count)
#else
#define ART_MEMSET(dest, c, count)          memset_s(dest, count, c, count)
#endif
#define ART_MEMMOVE(dest, src, count)       memmove_s(dest, count, src, count)
#define ART_MEMCMP(dest, src, count)        memcmp(dest, src, count)

#ifdef WIN32_DEBUG
enum DBBusinessType {
    kPelagoDBLunMap = 0x0,
    kPelagoDBFPMap = 0x1,
    kPelagoDBOPMap = 0x2,
    kPelagoDBSystemTable = 0x3,
    kPelagoDBFPInUseMap = 0x4,
    kPelagoDBBusinessOther = 0x5,
    kPelagoDBBusinessTypeButt = 0x6,
};
#endif

/*check the bitmap, return 0 or 1*/
static inline uint32_t art_check_bitmap(uint8_t *bitmap, uint32_t index)
{
    return (bitmap[index >> 3] & (1 << (index & 0x07))) ? 1 : 0;
}

/*set the bitmap to 0 or 1*/
static inline void art_set_bitmap(uint8_t *bitmap, uint32_t index, bool  value)
{
    /*set to 1*/
    if (value)
    {
        bitmap[index >> 3] |= (1 << (index & 0x07));
    }
    else
    {
        bitmap[index >> 3] &= ~(1 << (index & 0x07));
    }
}

/*clear the bitmap,that is set all bits of the bitmap to 0*/
static inline void art_clear_bitmap(uint8_t *bitmap, uint32_t bitmap_size)
{
    assert(0 == bitmap_size % 8);
    ART_MEMSET(bitmap, 0, bitmap_size >> 3);
}

/*bit 1 count, [0, end_index)*/
uint32_t get_bitmap_one_num(uint8_t *bitmap, uint32_t end_index, uint32_t max_bm_bits);

#ifdef WIN32_DEBUG
int gettimeofday(struct timeval *tp, void *tzp);
#endif

#if defined(_SCM_PCLINT_) || defined(WIN32_DEBUG)
typedef unsigned __int64 atomic64_t;

#define DBG_Print(...) printf("Filename %s, Function %s, Line %d > ", __FILE__, __FUNCTION__, __LINE__); \
                            printf(__VA_ARGS__); \
                            printf("\n");

#define DBG_PrintBuf(...) printf("Filename %s, Function %s, Line %d > ", __FILE__, __FUNCTION__, __LINE__); \
                            printf(__VA_ARGS__); \
                            printf("\n");

#define DBG_ShowUsageAndErrMsg(...) printf("Filename %s, Function %s, Line %d > ", __FILE__, __FUNCTION__, __LINE__); \
                            printf(__VA_ARGS__); \
                            printf("\n");

#define RETURN_OK 0
#define RETURN_ERROR 1
#define DBG_MAX_COMMAND_LEN   20
#define DBG_MAX_CMD_DESC_LEN  64

typedef void(*FN_DBG_CMD_PROC)(OSP_S32 v_iArgc, OSP_CHAR *v_szArgv[]);
typedef void(*FN_DBG_CMD_HELP_PROC)(OSP_CHAR *v_szCommand, OSP_S32 iShowDetail);

/** \brief 调试命令注册结构 */
typedef struct
{
	OSP_CHAR szCommand[DBG_MAX_COMMAND_LEN];              /**< 命令字, 长度[1,15] */
	OSP_CHAR szDescription[DBG_MAX_CMD_DESC_LEN];         /**< 命令的简要描述 */
	FN_DBG_CMD_PROC fnCmdDo;             /**< 命令执行函数,  v_szArgv[0] 是命令字 */
	FN_DBG_CMD_HELP_PROC fnPrintCmdHelp; /**< 命令打印帮助的函数 */
} DBG_CMD_S;

static int32_t DBG_RegCmd(DBG_CMD_S* cmd)
{
	UNREFERENCE_PARAM(cmd);

	return  RETURN_OK;
}

static void DBG_UnRegCmd(char* str)
{
	UNREFERENCE_PARAM(str);
}

static void atomic64_set(atomic64_t* v, int64_t s)
{
	*v = s;
}

static void atomic64_inc(atomic64_t* v)
{
	(*v)++;
}

static void atomic64_dec(atomic64_t* v)
{
    (*v)--;
}

static int64_t atomic64_read(atomic64_t* v)
{
	return (int64_t)(*v);
}

static void atomic64_add(int64_t i, atomic64_t* v)
{
	(*v) += i;
}

static int64_t atomic64_dec_return(atomic64_t* v)
{
    (*v)--;
    return *v;
}
#endif

#ifdef __cplusplus
}
#endif

#endif

