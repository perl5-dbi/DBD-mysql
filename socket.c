#ifdef _WIN32
#include "windows.h"
#include "winsock.h"
#endif

#ifndef _WIN32
#include <poll.h>
#include <errno.h>
#endif

#include <mysql.h>

/*
 * Warning: Native socket code must be outside of dbdimp.c and dbdimp.h because
 *          perl header files redefine socket function. This file must not
 *          include any perl header files!
 */

int mysql_socket_ready(my_socket fd)
{
  int retval;

#ifdef _WIN32
  /* Windows does not have poll(), so use select() instead */
  struct timeval timeout;
  fd_set fds;

  FD_ZERO(&fds);
  FD_SET(fd, &fds);

  timeout.tv_sec = 0;
  timeout.tv_usec = 0;

  retval = select(fd+1, &fds, NULL, NULL, &timeout);
#else
  struct pollfd fds;

  fds.fd = fd;
  fds.events = POLLIN;

  retval = poll(&fds, 1, 0);
#endif

  if (retval < 0) {
#ifdef _WIN32
    /* Windows does not update errno */
    return -WSAGetLastError();
#else
    return -errno;
#endif
  }

  return retval;
}
