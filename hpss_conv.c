/* static const char SccsId [] = "$URL: svn+ssh://jenova/var/svn/hpss/tags/7.5.1p1_f/src/cs/hpss_conv.c $ $Id: hpss_conv.c 19062 2016-07-20 03:07:54Z toennies $"; */
/*   begin_generated_IBM_prolog                                          */
/*                                                                       */
/*   This is an automatically generated prolog                           */
/*   ------------------------------------------------------------------  */
/*   Product(s): HPSS                                                    */
/*                                                                       */
/*   Licensed Materials                                                  */
/*                                                                       */
/*   Copyright (C) 1992, 2006                                            */
/*   International Business Machines Corporation, The Regents of the     */
/*   University of California, Los Alamos National Security, LLC,        */
/*   Lawrence Livermore National Security, LLC, Sandia Corporation,      */
/*   and UT-Battelle.                                                    */
/*                                                                       */
/*   All rights reserved.                                                */
/*                                                                       */
/*   Portions of this work were produced by Lawrence Livermore National  */
/*   Security, LLC, Lawrence Livermore National Laboratory (LLNL)        */
/*   under Contract No. DE-AC52-07NA27344 with the U.S. Department       */
/*   of Energy (DOE); by the University of California, Lawrence          */
/*   Berkeley National Laboratory (LBNL) under Contract                  */
/*   No. DE-AC02-05CH11231 with DOE; by Los Alamos National              */
/*   Security, LLC, Los Alamos National Laboratory (LANL) under          */
/*   Contract No. DE-AC52-06NA25396 with DOE; by Sandia Corporation,     */
/*   Sandia National Laboratories (SNL) under Contract                   */
/*   No. DE-AC04-94AL85000 with DOE; and by UT-Battelle, Oak Ridge       */
/*   National Laboratory (ORNL) under Contract No. DE-AC05-00OR22725     */
/*   with DOE.  The U.S. Government has certain reserved rights under    */
/*   its prime contracts with the Laboratories.                          */
/*                                                                       */
/*                             DISCLAIMER                                */
/*   Portions of this software were sponsored by an agency of the        */
/*   United States Government.  Neither the United States, DOE, The      */
/*   Regents of the University of California, Los Alamos National        */
/*   Security, LLC, Lawrence Livermore National Security, LLC, Sandia    */
/*   Corporation, UT-Battelle, nor any of their employees, makes any     */
/*   warranty, express or implied, or assumes any liability or           */
/*   responsibility for the accuracy, completeness, or usefulness of     */
/*   any information, apparatus, product, or process disclosed, or       */
/*   represents that its use would not infringe privately owned rights.  */
/*                                                                       */
/*   High Performance Storage System is a trademark of International     */
/*   Business Machines Corporation.                                      */
/*   ------------------------------------------------------------------  */
/*                                                                       */
/*   end_generated_IBM_prolog                                            */
/*============================================================================
 *
 *  Name: hpss_conv.c
 *
 *  Functions: atobytes		- convert string to long
 *             atobytesll	- convert string to long long
 *             atooctal		- convert octal string to long
 *             hpss_ConvertPosixModeToMode - convert posix mode to hpss mode
 *             bits
 *             hpss_ConvertModeToPosixMode - convert hpss mode to posix mode
 *             bits
 *
 *  Description:
 *       The functions listed above are used to convert strings to numbers and
 *       other common conversions.
 *  
 *  Traceability:
 *       Version   Date       Description
 *       -------   --------   --------------
 *        1.1-5    08/23/06   tjp-5768 Initial version
 *        1.6      08/25/06   tjp-5791 define LLONG_MAX for IRIX
 *        1.7      10/06/06   tjp-5856 Change _toupper to toupper
 *        1.8      10/31/06   tjp-5930 Add additional input checks
 *
 * Bugzilla:
 *       Date       Description
 *       --------   --------------
 *       05/25/11   tjp-1110 Don't redefine LLONG_MAX for Linux
 *       07/06/11   tjp-1215 Delete IRIX support
 *       09/12/11   tjp-1395 Correct definition of __USE_ISOC99
 *       10/26/11   tjp-1511 Delete __USE_ISOC99
 *       06/04/14   mfm-3994 Moved common mode conversion routines here
 *
 *  Notes:
 *      This file is IBM Confidential.
 *
 *========================================================================== */

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#if defined(LINUX)
#include <string.h>
#endif
#include "hpss_errno.h"
#include "hpss_conv.h"
#include "hpss_attrs.h"
#include "ns_Constants.h"

/*============================================================================
 *
 * Function:   atobytesll() - convert string to long long
 *
 * Synopsis:
 *   long long
 *   atobytesll(
 *      const char *s,		** IN  - String to be converted
 *      int  *error)		** OUT - Error code from operation
 *
 * Description:
 *   Converts a string of the form ###Unit (e.g., 1.5mb) to a long long
 *
 * Outputs:
 *   long long value if no error(s) occur, else returns 0
 *
 * Interfaces:
 *   free()
 *   isdigit()
 *   sscanf()
 *   strdup()
 *   strlen()
 *   strchr()
 *   toupper()
 *
 * Resources Used:
 *   None.
 *
 * Limitations:
 *   Precision is limited to 3 decimal points
 *
 * Assumptions:
 *   None.
 *
 * Notes:
 *   Interprets the following special characters in the string:
 *      K/KB  - kilobytes
 *      M/MB  - megabytes
 *      G/GB  - gigabytes
 *      T/TB  - terabytes
 *      P/PB  - petabytes
 *      X/XB  - exabytes
 *===========================================================================*/

long long
atobytesll (
   const char *s,	/* IN - input string to be converted */
   int  *error)		/* OUT - error code */
{
   double       x = 0.0;
   char        *tmp_s, *pos;
   int          sign, signoff;
   long long    result = 0;
   enum {ZERO = 0, KB = 10, MB = 20, GB = 30,
         TB = 40, PB = 50, XB = 60} shift_factor;
   size_t       len;

   /*
    *  Check for valid input
    */
   if (s == NULL)
   {
      *error = HPSS_EFAULT;
      return 0;
   }
   if (*s == '\0')
   {
      *error = HPSS_EINVAL;
      return 0;
   }

   /*
    * Copy s so it can be modified
    */
   if ((tmp_s = strdup(s)) == NULL)
   {
      *error = HPSS_ENOMEM;
      return 0;
   }
   switch (*tmp_s)
   {
      case '-':
         sign = -1;
         signoff = 1;
         break;
      case '+':
         sign = 1;
         signoff = 1;
         break;
      default:
         sign = 1;
         signoff = 0;
   }
   if (tmp_s[signoff] == '\0')
   {
      *error =  HPSS_EINVAL;
      result = 0;
      goto done;
   }

   /*
    * First check for two-letter units (e.g., "kb"),
    * then look for single-letter units (e.g., "k")
    */
   len = strlen(&tmp_s[signoff]);
   if (len ==1)
      pos = &tmp_s[signoff];
   else
   {
      pos = &tmp_s[len+signoff-2];
      if (toupper(*(pos+1)) != 'B')
         ++pos;
   }
   switch (toupper(*pos))
   {
      case 'B':
         shift_factor = ZERO;
         *pos = '\0';
         break;
      case 'K':
         shift_factor = KB;
         break;
      case 'M':
         shift_factor = MB;
         break;
      case 'G':
         shift_factor = GB;
         break;
      case 'T':
         shift_factor = TB;
         break;
      case 'P':
         shift_factor = PB;
         break;
      case 'X':
         shift_factor = XB;
         break;
      default:
         if (isdigit(*pos))
            shift_factor = ZERO;
         else
         {
            *error =  HPSS_EINVAL;
            result = 0;
            goto done;
         }
   }

   if (shift_factor != ZERO)
      *pos = '\0';
   *error = HPSS_E_NOERROR;

   /*
    * If no decimal point use integer
    */
   if (strchr(&tmp_s[signoff], '.') == NULL)
   {
      if (sscanf(&tmp_s[signoff], "%lld", &result) != 1)
      {
         *error = -errno;
         result = 0;
      }
      else
      {
         long long tresult = result;

         result <<= shift_factor;
         /* Overflow? */
         if ((result >> shift_factor) != tresult)
         {
            *error = HPSS_ERANGE;
            result = 0;
         }
         result *= sign;
      }
   }
   else
   {
      if (sscanf(&tmp_s[signoff], "%lf", &x) != 1)
      {
         *error = -errno;
         result = 0;
      }
      else
      {
         result = x * 1000;    /* handle up to three decimal places */
         x = (double) result / 1000;
         x *= (long long)1 << shift_factor;
         /* Overflow? */
         if (x > LLONG_MAX)
         {
            *error = HPSS_ERANGE;
            result = 0;
         }
         else
            result = sign * x;
      }
   }

done:
   free(tmp_s);
   return result;
} /* end atobytesll */

/*============================================================================
 *
 * Function:   atobytes()
 *
 * Synopsis:
 *   long
 *   atobytes(
 *      const char *s)	** IN - String to be interpreted
 *
 * Description:
 *   Converts a string of the form ###Unit (e.g., 1.5mb) to a long.
 * 
 * Outputs:
 *   long value if no error(s) occur, else returns 0
 *
 * Interfaces:
 *   atobytesll()
 *
 * Resources Used:
 *   None
 *
 * Limitations:
 *   See atobytesll
 *
 * Assumptions:
 *   None
 *
 * Notes:
 *   See atobytesll
 *
 *-------------------------------------------------------------------------*/
long 
atobytes (
   const char *s)	/* IN - String to be interpreted */
{
   int		error;
   long long	llret;

   llret = atobytesll(s, &error);
   if (error || (llret > LONG_MAX) || (llret < -LONG_MAX))
      return 0;
   else
      return (long)llret;
} /* end atobytes */

/*============================================================================
 *
 * Function:   atooctal()
 *
 * Synopsis:
 *   long
 *   atooctal(
 *      const char *s)	** IN - String to be converted
 *
 * Description:
 *   Converts an octal string to a long.
 * 
 * Outputs:
 *   long value if no error(s) occur, else returns 0
 *
 * Interfaces:
 *   isspace()
 *
 * Resources Used:
 *   None
 *
 * Limitations:
 *   None
 *
 * Assumptions:
 *   None
 *
 * Notes:
 *   None
 *
 *-------------------------------------------------------------------------*/
long
atooctal(
   const char *s)	/* IN - String to be converted */
{
   int		result = -1;
   int		retVal = 0;

   /* Skip leading whitespace) */
   while (isspace(*s))
      s++;

   /* Exit if null string */
   if (!*s)
      goto done;	

   /*
    * Convert string up to '\0'. Quit if bad octal digit detected
    */
   while (*s)
   {
      if ((*s < '0') || (*s > '7'))
         goto done;
      retVal = (retVal << 3) | (*s - '0');
      ++s;
   }
   result = retVal;

done:
   return result;
}

/**
 *
 * @brief Convert POSIX mode bits into HPSS attrs
 *
 * @param[in]      PosixMode   Posix Mode Bits
 * @param[in,out]  Attrs       HPSS Attributes
 * 
 */
void 
hpss_ConvertPosixModeToMode(
mode_t          PosixMode,
hpss_Attrs_t    *Attrs)
{

   /*
    *  File type.
    */

   if (S_ISDIR(PosixMode))
      Attrs->Type = NS_OBJECT_TYPE_DIRECTORY;
   else if (S_ISREG(PosixMode))
      Attrs->Type = NS_OBJECT_TYPE_FILE;
   else if (S_ISLNK(PosixMode))
      Attrs->Type = NS_OBJECT_TYPE_SYM_LINK;

   /*
    *  Set uid/gid,sticky bits.
    */

   if (PosixMode & S_ISUID)
      Attrs->ModePerms |= NS_PERMS_RD;
   if (PosixMode & S_ISGID)
      Attrs->ModePerms |= NS_PERMS_WR;
   if (PosixMode & S_ISVTX)
      Attrs->ModePerms |= NS_PERMS_XS;


   /*
    *  Permission bits.
    */

   if (PosixMode & S_IRUSR)
      Attrs->UserPerms |= NS_PERMS_RD;
   if (PosixMode & S_IWUSR)
      Attrs->UserPerms |= NS_PERMS_WR;
   if (PosixMode & S_IXUSR)
      Attrs->UserPerms |= NS_PERMS_XS;
   if (PosixMode & S_IRGRP)
      Attrs->GroupPerms |= NS_PERMS_RD;
   if (PosixMode & S_IWGRP)
      Attrs->GroupPerms |= NS_PERMS_WR;
   if (PosixMode & S_IXGRP)
      Attrs->GroupPerms |= NS_PERMS_XS;
   if (PosixMode & S_IROTH)
      Attrs->OtherPerms |= NS_PERMS_RD;
   if (PosixMode & S_IWOTH)
      Attrs->OtherPerms |= NS_PERMS_WR;
   if (PosixMode & S_IXOTH)
      Attrs->OtherPerms |= NS_PERMS_XS;
}

/*===========================================================================*/
/**
 * \brief Converts an input HPSS mode value to its corresponding POSIX mode value.
 *
 * \details  This routine translates an HPSS mode value to the equivalent POSIX
 *          mode_t value.
 *
 * \param[in]  Attrs                         HPSS object attributes
 * \param[out] PosixMode                     Ptr to posix mode_t
 *
 * \retval None. 
 */
 /*---------------------------------------------------------------------------*/
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

