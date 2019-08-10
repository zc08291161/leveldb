#ifndef _SCM_PCLINT_
#include "art_ftds.h"
#include "art_pub.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif

#ifdef ARTREE_DEBUG
void start_comm_ftds(uint32_t point, uint64_t *ftds_timeStart)
{
    UNREFERENCE_PARAM(point);
    UNREFERENCE_PARAM(ftds_timeStart);
    return;
}

void end_comm_ftds(uint32_t point, uint32_t status, uint64_t ftds_timeStart)
{
    UNREFERENCE_PARAM(point);
    UNREFERENCE_PARAM(status);
    UNREFERENCE_PARAM(ftds_timeStart);
    return;
}
#endif

#ifdef __cplusplus
}
#endif

