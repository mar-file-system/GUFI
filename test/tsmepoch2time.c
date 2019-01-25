#include <stdio.h>
#include <time.h>

int main(int argc, char **argv)
{
    time_t     now;
    struct tm  ts;
    char       buf[80];

    // Get current time
    //time(&now);
    strptime(argv[1], "%s", &ts);

    // Format time, "ddd yyyy-mm-dd hh:mm:ss zzz"
    //ts = *localtime(&now);
    //strftime(buf, sizeof(buf), "-fromtime=%m/%d/%Y -fromtime=%H:%M%p", &ts);
    strftime(buf, sizeof(buf), "-fromtime=%m/%d/%Y -fromtime=%H:%M", &ts);
    printf("%s\n", buf);
    return 0;
}
