#define _XOPEN_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int main(int argc, char **argv)
{
    struct tm t;
    time_t t_of_day;
    int myear;

    sscanf(argv[1],"%d/%d/%d",&t.tm_mon,&t.tm_mday,&myear);
    sscanf(argv[2],"%d:%d:%d",&t.tm_hour,&t.tm_min,&t.tm_sec);
    t.tm_year=myear-1900;
    t.tm_isdst = -1;        // Is DST on? 1 = yes, 0 = no, -1 = unknown
    t_of_day = mktime(&t);

    printf("seconds since the Epoch: %ld\n", (long) t_of_day);
}
