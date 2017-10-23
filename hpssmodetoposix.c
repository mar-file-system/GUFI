void
hpss_ConvertModeToPosixMode(
const hpss_Attrs_t    *Attrs,
mode_t                *PosixMode)
{
   *PosixMode = (mode_t)0;

   /*
    *  File type.
    */

   switch(Attrs->Type)
   {
      case NS_OBJECT_TYPE_DIRECTORY:
      case NS_OBJECT_TYPE_JUNCTION:
         *PosixMode |= S_IFDIR;
         break;

      case NS_OBJECT_TYPE_FILE:
      case NS_OBJECT_TYPE_HARD_LINK:
         *PosixMode |= S_IFREG;
         break;

      case NS_OBJECT_TYPE_SYM_LINK:
         *PosixMode |= S_IFLNK;
         break;
   }

   /*
    *  Set uid/gid, sticky bits.
    */

   if (Attrs->ModePerms & NS_PERMS_RD)
      *PosixMode |= S_ISUID;
   if (Attrs->ModePerms & NS_PERMS_WR)
      *PosixMode |= S_ISGID;
   if (Attrs->ModePerms & NS_PERMS_XS)
      *PosixMode |= S_ISVTX;

   /*
    *  Permission bits.
    */

   if (Attrs->UserPerms & NS_PERMS_RD)
      *PosixMode |= S_IRUSR;
   if (Attrs->UserPerms & NS_PERMS_WR)
      *PosixMode |= S_IWUSR;
   if (Attrs->UserPerms & NS_PERMS_XS)
      *PosixMode |= S_IXUSR;
   if (Attrs->GroupPerms & NS_PERMS_RD)
      *PosixMode |= S_IRGRP;
   if (Attrs->GroupPerms & NS_PERMS_WR)
      *PosixMode |= S_IWGRP;
   if (Attrs->GroupPerms & NS_PERMS_XS)
      *PosixMode |= S_IXGRP;
   if (Attrs->OtherPerms & NS_PERMS_RD)
      *PosixMode |= S_IROTH;
   if (Attrs->OtherPerms & NS_PERMS_WR)
      *PosixMode |= S_IWOTH;
   if (Attrs->OtherPerms & NS_PERMS_XS)
      *PosixMode |= S_IXOTH;

   return;
}

