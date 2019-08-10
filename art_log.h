/*******************************************************************
                   Copyright 2017 - 2020, Huawei Tech. Co., Ltd.
                             ALL RIGHTS RESERVED

Filename      : art_log.h
Author        : 
Creation time : 2018/4/11
Description   : ARTree index

Version       : 
********************************************************************/
#ifndef ART_LOG_H
#define ART_LOG_H

#ifndef ARTREE_DEBUG
/* 根据需求修改相应的头文件 */
#include "lvos.h"
#include "pid_pelagodb.h"
#else
#include <stdlib.h>
#include <stdio.h>
#endif

#ifdef __cplusplus
    extern "C" {
#endif

#ifndef ARTREE_DEBUG
#define ART_LOGINFO(LOGID, ...) \
    (DBG_LogIntf(DBG_LOG_INFO, (LOGID), "ART "__VA_ARGS__))
#define ART_LOGDEBUG(LOGID, ...) \
    (DBG_LogIntf(DBG_LOG_DEBUG, (LOGID), "ART "__VA_ARGS__))
#define ART_LOGWARNING(LOGID, ...) \
    (DBG_LogIntf(DBG_LOG_WARNING, (LOGID), "ART "__VA_ARGS__))
#define ART_LOGERROR(LOGID, ...) \
    (DBG_LogIntf(DBG_LOG_ERROR, (LOGID), "ART "__VA_ARGS__))
#else
#define ART_LOGINFO(LOGID, ...) 
/*
     printf("[INFO][ART] "__VA_ARGS__);\
     printf(".\n");
*/
#define ART_LOGDEBUG(LOGID, ...) 
/*
     printf("[DEBUG][ART] "__VA_ARGS__);\
     printf(".\n");
*/
#define ART_LOGWARNING(LOGID, ...) 
/*
     printf("[WARNING][ART] "__VA_ARGS__);\
     printf(".\n");
*/
#define ART_LOGERROR(LOGID, ...) 
/*
     printf("[ERROR][ART] "__VA_ARGS__);\
     printf(".\n");
*/
#ifdef WIN32
#define INDEX_LOG_INFO_LIMIT(format, ...)
#define INDEX_LOG_WARN_LIMIT(format, ...)
#endif

#ifdef WIN32
#define INDEX_LOG_WARN_LIMIT(format, ...)
#endif

#endif
/*********************** 日志 ID ************************/
/*打印日志需要添加日志ID*/
#define ART_LOG_ID_4B00 0x4B00U


#ifdef __cplusplus
}
#endif
/** @} */
/** @} */
#endif

