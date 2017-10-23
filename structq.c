 struct Node
 {
        char Data[MAXPATH];
        struct stat status;
        int pinode;
        char Datax[MAXXATTR];
        struct Node* next;
 }*rear, *front;

 int qent;

void delQueue()
{
       struct Node *temp, *var=rear;
      if(var==rear)
      {
             rear = rear->next;
             free(var);
             if (rear==NULL) front=NULL;
             qent--;
      }
      else
      printf("\n delQueue Empty");
}

void push(char *value, struct stat *status, int pinode)
{
     struct Node *temp;
     temp=(struct Node *)malloc(sizeof(struct Node));
     qent++;
     sprintf(temp->Data,"%s",value);
     temp->status.st_ino=status->st_ino;
     temp->status.st_mode=status->st_mode;
     temp->status.st_nlink=status->st_nlink;
     temp->status.st_uid=status->st_uid;
     temp->status.st_gid=status->st_gid;
     temp->status.st_size=status->st_size;
     temp->status.st_blksize=status->st_blksize;
     temp->status.st_blocks=status->st_blocks;
     temp->status.st_atime=status->st_atime;
     temp->status.st_mtime=status->st_mtime;
     temp->status.st_ctime=status->st_ctime;
     temp->pinode=pinode;
     if (front == NULL)
     {
           front=temp;
           front->next=NULL;
           rear=front;
     }
     else
     {
           front->next=temp;
           front=temp;
           front->next=NULL;
     }
}

void pushx(char *value, struct stat *status, int pinode, char *valuex)
{
     struct Node *temp;
     temp=(struct Node *)malloc(sizeof(struct Node));
     qent++;
     sprintf(temp->Data,"%s",value);
     temp->status.st_ino=status->st_ino;
     temp->status.st_mode=status->st_mode;
     temp->status.st_nlink=status->st_nlink;
     temp->status.st_uid=status->st_uid;
     temp->status.st_gid=status->st_gid;
     temp->status.st_size=status->st_size;
     temp->status.st_blksize=status->st_blksize;
     temp->status.st_blocks=status->st_blocks;
     temp->status.st_atime=status->st_atime;
     temp->status.st_mtime=status->st_mtime;
     temp->status.st_ctime=status->st_ctime;
     temp->pinode=pinode;
     sprintf(temp->Datax,"%s",valuex);
     if (front == NULL)
     {
           front=temp;
           front->next=NULL;
           rear=front;
     }
     else
     {
           front->next=temp;
           front=temp;
           front->next=NULL;
     }
}

void display()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           printf("\nElements are as:  ");
           while(var!=NULL)
           {
                printf("\t%s",var->Data);
                var=var->next;
           }
     printf("\n");
     } 
     else
     printf("\ndisplayQueue is Empty");
}

void displaycurrent()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
                printf("current: \t%s",var->Data);
     printf("\n");
     } 
     else
     printf("\ndisplaycurrentQueue is Empty");
}

void displayqent()
{
     printf("qent = %d\n",qent);
}

int addrqent()
{
      return qent;
}

char * addrcurrent()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           return var->Data;
     }
     else
     printf("\naddrcurrentQueue is Empty");
     return NULL;
}

char * addrcurrentx()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           return var->Datax;
     }
     else
     printf("\naddrcurrentxQueue is Empty");
     return NULL;
}

int addrcurrentp()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           return var->pinode;
     }
     else
     printf("\naddrcurrentpQueue is Empty");
     return 0;
}

struct stat * addrcurrents()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           return &var->status;
     }
     else
     printf("\naddrcurrentsQueue is Empty");
     return NULL;
}

