/* Generic ctype.h */
/* $OpenLDAP: pkg/ldap/include/ac/ctype.h,v 1.16.2.6 2011/01/04 23:49:56 kurt Exp $ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2011 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#ifndef _AC_CTYPE_H
#define _AC_CTYPE_H

#include <ctype.h>

#undef TOUPPER
#undef TOLOWER

#ifdef C_UPPER_LOWER
# define TOUPPER(c)	(islower(c) ? toupper(c) : (c))
# define TOLOWER(c)	(isupper(c) ? tolower(c) : (c))
#else
# define TOUPPER(c)	toupper(c)
# define TOLOWER(c)	tolower(c)
#endif

#endif /* _AC_CTYPE_H */
