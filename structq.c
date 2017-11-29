struct Node
{
        struct work swork;
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

void delQueuenofree()
{
      struct Node *temp, *var=rear;
      if(var==rear)
      {
             rear = rear->next;
             //free(var);
             if (rear==NULL) front=NULL;
             qent--;
      }
      else
      printf("\n delQueue Empty");
}

void pushn(struct work *twork)
{
     struct Node *temp;
     temp=(struct Node *)malloc(sizeof(struct Node));
     qent++;
     bcopy (twork,&temp->swork,sizeof(struct work));
     temp->swork.freeme=temp;
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
                printf("\t%s",var->swork.name);
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
                printf("current: \t%s",var->swork.name);
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

struct work * addrcurrents()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           return &var->swork;
     }
     else
     printf("\naddrcurrentsQueue is Empty");
     return NULL;
}

