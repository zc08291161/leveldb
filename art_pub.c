#include "art_pub.h"
#ifdef WIN32_DEBUG
#include <time.h>
#include <windows.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WIN32_DEBUG
int gettimeofday(struct timeval *tp, void *tzp)
{
	time_t c;
	struct tm tm;
	SYSTEMTIME wtm;
	UNREFERENCE_PARAM(tzp);
	GetLocalTime(&wtm);
	tm.tm_year = wtm.wYear - 1900;
	tm.tm_mon = wtm.wMonth - 1;
	tm.tm_mday = wtm.wDay;
	tm.tm_hour = wtm.wHour;
	tm.tm_min = wtm.wMinute;
	tm.tm_sec = wtm.wSecond;
	tm.tm_isdst = -1;
	c = mktime(&tm);
	tp->tv_sec = (long)c;
	tp->tv_usec = wtm.wMilliseconds * 1000;

	return (0);
}
#endif


/*bit 1 count, [0, end_index)*/
uint32_t get_bitmap_one_num(uint8_t *bitmap, uint32_t end_index, uint32_t max_bm_bits)
{
    uint32_t i = 0;
    uint32_t slot = 0;
    uint32_t count = 0;

    if(0 == end_index)
    {
        return 0;
    }
    
    assert(max_bm_bits % 8 == 0);
    
    slot = end_index / 8;
    for (i = 0; i < slot; i++)
    {
        count += g_one_num_table[bitmap[i] & 0xFF];
    }
    if (end_index != max_bm_bits)
    {
        count += g_one_num_table[bitmap[i] & (0xFF >> (8 - (end_index % 8)))];
    }
    
    return count;
}


#ifdef __cplusplus
}
#endif

