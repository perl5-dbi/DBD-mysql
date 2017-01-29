#ifndef _WIN32

#include <poll.h>
#include <errno.h>

#include <mysql.h>

/*
 * Warning: Native socket code must be outside of dbdimp.c and dbdimp.h because
 *          perl header files redefine socket function. This file must not
 *          include any perl header files!
 */

int mysql_socket_ready(my_socket fd)
{
  int retval;
  struct pollfd fds;

  fds.fd = fd;
  fds.events = POLLIN;

  retval = poll(&fds, 1, 0);

  if (retval < 0) {
    return -errno;
  }

  return retval;
}

#endif
