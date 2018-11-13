#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <vector>

#define DEF_UNUSED(x) (void)x;

#if defined(_WIN32)
#include <Windows.h>

inline void CopyToBuffer(const char* utf8Src, size_t len, std::vector<char>& dst)
{
	dst.resize(len);
	memcpy(&dst[0], utf8Src, len);
}

inline void ConvertUtf8ToAnsi(const char* src, std::vector<char>& dst)
{
	dst.clear();
	const size_t srcLen = strlen(src) + 1;

	const int len1 = MultiByteToWideChar(CP_UTF8, 0, src, static_cast<int>(srcLen), NULL, 0);
	if (len1 <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	std::vector<WCHAR> utf16Buffer(len1);
	int res = MultiByteToWideChar(CP_UTF8, 0, src, static_cast<int>(srcLen), &utf16Buffer[0], static_cast<int>(utf16Buffer.size()));
	if (res <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	const int len2 = WideCharToMultiByte(CP_ACP, 0, &utf16Buffer[0], static_cast<int>(utf16Buffer.size()), NULL, 0, NULL, NULL);
	if (len2 <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	char lpDefaultChar = '_';
	BOOL lpUsedDefaultChar = FALSE;
	dst.resize(len2);
	res = WideCharToMultiByte(CP_ACP, 0, &utf16Buffer[0], static_cast<int>(utf16Buffer.size()), &dst[0], static_cast<int>(dst.size()), &lpDefaultChar, &lpUsedDefaultChar);
	if (res <= 0 || lpUsedDefaultChar)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}
}

inline void ConvertAnsiToUtf8(const char* src, std::vector<char>& dst)
{
	dst.clear();
	const size_t srcLen = strlen(src) + 1;

	const int len1 = MultiByteToWideChar(CP_ACP, 0, src, static_cast<int>(srcLen), NULL, 0);
	if (len1 <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	std::vector<WCHAR> utf16Buffer(len1);
	int res = MultiByteToWideChar(CP_ACP, 0, src, static_cast<int>(srcLen), &utf16Buffer[0], static_cast<int>(utf16Buffer.size()));
	if (res <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	const int len2 = WideCharToMultiByte(CP_UTF8, 0, &utf16Buffer[0], static_cast<int>(utf16Buffer.size()), NULL, 0, NULL, NULL);
	if (len2 <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}

	dst.resize(len2);
	res = WideCharToMultiByte(CP_UTF8, 0, &utf16Buffer[0], static_cast<int>(utf16Buffer.size()), &dst[0], static_cast<int>(dst.size()), NULL, NULL);
	if (res <= 0)
	{
		CopyToBuffer(src, srcLen, dst);
		return;
	}
}

#endif

inline const char* GetUTFChars(JNIEnv* env, jstring source, std::vector<char>& buffer)
{
#if defined(_WIN32)
	const char* utf8Str = env->GetStringUTFChars(source, nullptr);
	if (utf8Str == nullptr)
	{
		return nullptr;
	}

	ConvertUtf8ToAnsi(utf8Str, buffer);
	env->ReleaseStringUTFChars(source, utf8Str);
	return &buffer[0];
#else
    DEF_UNUSED(buffer)
	return env->GetStringUTFChars(source, nullptr);
#endif
}

inline void ReleaseUTFChars(JNIEnv* env, jstring source, const char* utfString)
{
#if defined(_WIN32)
	// do nothing
#else
	env->ReleaseStringUTFChars(source, utfString);
#endif
}

inline const char* ConvertFromSystemEncodingToUtf8(const char* str, std::vector<char>& buffer)
{
	buffer.clear();
	if (str == nullptr)
	{
		return nullptr;
	}

#if defined(_WIN32)
	ConvertAnsiToUtf8(str, buffer);
	return &buffer[0];
#else
	return str;
#endif
}
