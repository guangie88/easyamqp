#pragma once

#if !defined(DLL_EASYAMQP_IMPORT) && !defined(DLL_EASYAMQP_EXPORT)
#   define DLL_EASYAMQP_EXPORT
#endif

#ifndef DLL_EASYAMQP
#   ifdef _WIN32
#       ifdef DLL_EASYAMQP_EXPORT
#           define DLL_EASYAMQP __declspec(dllexport)
#       else
#           define DLL_EASYAMQP __declspec(dllimport)
#       endif
#   else
#       define DLL_EASYAMQP
#   endif
#endif