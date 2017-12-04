int processdirs() {

     int myqent;
     struct work * workp;

     // loop over queue entries and running threads and do all work until running threads zero and queue empty
     myqent=0;
     runningthreads=0;
     while (1) {
        myqent=getqent();
        if (myqent == 0) {
          if (runningthreads == 0) {
               myqent=getqent();
               if (myqent == 0) {
                 break;
               }
          }
        }
        if (runningthreads < in.maxthreads) {
          if (myqent > 0) {
            // we have to lock around all queue ops
            pthread_mutex_lock(&queue_mutex);
            workp=addrcurrents();
            incrthread();
            // this takes this entry off the queue but does NOT free the buffer, that has to be done in processdir()  free (_.freeme)
            delQueuenofree();
            thpool_add_work(mythpool, (void*)processdir,(void *)workp);
            //delQueuenofree();
            pthread_mutex_unlock(&queue_mutex);
          }
        }
     }

     return 0;
}
