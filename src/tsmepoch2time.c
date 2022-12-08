#define _XOPEN_SOURCE
#include <stdio.h>
#include <time.h>

int main(int argc, char **argv)
{
    struct tm  ts;
    char       buf[80];

    strptime(argv[1], "%s", &ts);

    // Format time, "ddd yyyy-mm-dd hh:mm:ss zzz"
    strftime(buf, sizeof(buf), "-fromtime=%m/%d/%Y -fromtime=%H:%M", &ts);
    printf("%s\n", buf);
    return 0;
}
