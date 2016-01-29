#include "stdio.h"
#include "stdlib.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "fcntl.h"
#include "sys/ioctl.h"
#include "string.h"
#include "errno.h"
#include "unistd.h"
#include "LoongStorePolicy.h"
#include "json/value.h"
#include "json/json.h"
#include <string>
#include <iostream>

#define MAX_FILTERS     10

using namespace std;
#define LEOFS_ERR_REP_ONLY          (1495) /*非副本存储模式*/
#define LEOFS_ERR_NOT_FILEDIR       (1496) /*既不是普通文件也不是目录*/
#define LEOFS_ERR_AFFINITY_NOTSET   (1497) /*查询的对象没有设置亲和性*/

struct __ioctl_get_istinfo {
	int istnum;
	int pad;
	int istid[16];
	char  ips[16][4][16];
};
typedef struct __ioctl_get_istinfo ioctl_get_istinfo_t;

#define LEOFS_IOC_SET_ALLOC_AFFINITY     _IO('l', 157)
#define LEOFS_IOC_CANCEL_ALLOC_AFFINITY  _IO('l', 158)
#define LEOFS_IOC_GET_ISTID              _IOR('l', 159, ioctl_get_istinfo_t)
#define LEOFS_IOC_CHECK_ALLOC_AFFINITY   _IO('l', 160)

jstring stringtoJstring( JNIEnv* env, const char* pat )
{
 jclass strClass = (env)->FindClass("Ljava/lang/String;");
 jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
 jbyteArray bytes = (env)->NewByteArray(strlen(pat));
 (env)->SetByteArrayRegion(bytes, 0, strlen(pat), (jbyte*)pat);
 jstring encoding = (env)->NewStringUTF("GB2312");
 return (jstring)(env)->NewObject(strClass, ctorID, bytes, encoding);
}

string jstringTostring(JNIEnv*   env,   jstring   jstr)
{
   char*   rtn   = NULL;
   jclass   clsstring   = env->FindClass("java/lang/String");
   jstring   strencode   = env->NewStringUTF("GB2312");
   jmethodID   mid   = env->GetMethodID(clsstring,"getBytes","(Ljava/lang/String;)[B");
   jbyteArray   barr= (jbyteArray)env->CallObjectMethod(jstr,mid,strencode);
   jsize   alen   = env->GetArrayLength(barr);
   jbyte* ba   = env->GetByteArrayElements(barr,JNI_FALSE);
   if(alen   >   0)
   {
        rtn   =   (char*)malloc(alen+1);
        memcpy(rtn,ba,alen);
        rtn[alen]=0;
   }
   env->ReleaseByteArrayElements(barr,ba,0);
   string stemp(rtn);
    free(rtn);
   return   stemp;
 }

int setAllocAffinity(string path){
    const char *pathPointer = path.c_str();
    int fd = -1;
    fd = open(pathPointer, O_RDONLY);
    if (fd < 0) {
    	cout<<"Open path: "<<path<<" failed,  errno: "<<errno<<"\n";
    	return errno;
    }
    int err = ioctl(fd, LEOFS_IOC_SET_ALLOC_AFFINITY);
    if (err < 0) {
        if (errno == LEOFS_ERR_NOT_FILEDIR) {
	        cout<<"Path "<<path<<"not regular file or directory!\n";
        } else if (errno == LEOFS_ERR_REP_ONLY) {
    	    cout<<"Path "<< path<<" not in rep layout!\n";
        } else {
		cout<<"Operation failed, errno: "<<errno<<endl;
        }
        return errno;
    }
    cout<<"Operation finished!\n";
    close(fd);
    return 0;
}
string makeErrorJson(int errcode){
     Json::Value root;
     root["errorCode"]= errcode;
     return root.toStyledString();
}
string makelstInfoJson(ioctl_get_istinfo_t  igid){
    Json::Value root;
    root["errorCode"]= 0;
    root["istnum"]=igid.istnum;
    root["pad"]=igid.pad;

    Json::Value istids;
    Json::Value ips;
    for (int i=0; i<igid.istnum; i++) {
        Json::Value istidRoot;
        Json::Value ipRoot;
	    char tmpbufp[128];
    	memset(tmpbufp, 0, 128);
    	char ip[80];
    	for (int j=0; j<4; j++) {
    		if (strlen(igid.ips[i][j]) <= 0) {
    			break;
    		}
    		sprintf(tmpbufp, "%s;%s", tmpbufp, igid.ips[i][j]);
    	}
        istidRoot["istid"]=igid.istid[i];
        ipRoot["ip"]=tmpbufp;

        istids.append(istidRoot);
        ips.append(ipRoot);
        printf("%s\n",tmpbufp);
    }
    root["istids"]=istids;
    root["ips"]=ips;
    return root.toStyledString();
}


string getlstInfo(string path,int offset){
    struct stat st;
	ioctl_get_istinfo_t  igid;
    const char *pathPointer = path.c_str();
    memset(&igid, 0, sizeof(ioctl_get_istinfo_t));
    int fd = open(pathPointer, O_RDONLY);
    if (fd < 0) {
    	cout<<"Open path: "<<path<<" failed,  errno:"<<errno<<endl;
        return makeErrorJson(errno);
    }
    memset(&st, 0, sizeof(st));
    int err = fstat(fd, &st);
    if (err < 0) {
	cout <<"Stat "<<path << "failed, errno:" << errno<<endl;
    	return makeErrorJson(errno);
    }
    if ((offset > 0) && S_ISREG(st.st_mode)) {
    	lseek(fd, offset, SEEK_SET);
    }
    err = ioctl(fd, LEOFS_IOC_GET_ISTID, &igid);
    if (err < 0) {
    	if (errno == EPERM) {
    		printf("Operation not permitted!\n");

    	} else if (errno == LEOFS_ERR_NOT_FILEDIR) {
		    cout<<"Path: "<<path << "is not regular file or directory!\n";
    	} else {
    		if (S_ISDIR(st.st_mode) && errno == LEOFS_ERR_AFFINITY_NOTSET) {
    			cout<<"Directory: " <<path<<" is not set alloc affinity!\n";
    		} else {
			cout<<"Operation failed, errno:"<<errno<<endl;
    		}
    	}
    	return makeErrorJson(errno);
    }

    if (S_ISREG(st.st_mode)) {
    	cout<<"File: "<<path<<", offset: "<<offset<<", istid number: "<<igid.istnum;
    } else {
	    cout<<"Directory: "<<path <<", istid number:"<<igid.istnum<<endl;
    }
    close(fd);
    return makelstInfoJson(igid);

}


JNIEXPORT jint JNICALL Java_LoongTest_setAllocAffinity(JNIEnv *env, jclass cls, jstring path) {
    string pathConvert = jstringTostring(env,path);
    return setAllocAffinity(pathConvert);
}


JNIEXPORT jstring JNICALL Java_LoongTest_getIstInfo(JNIEnv *env, jclass cls, jstring path, jint offset) {
     string pathConvert = jstringTostring(env,path);
     string retContent = getlstInfo(pathConvert,offset);
     return stringtoJstring(env,retContent.c_str());
}

JNIEXPORT jstring JNICALL Java_LoongTest_getIpNodeMap(JNIEnv *env, jclass cls) {
    int fd;
    //fd = shm_open("/etc/hosts", O_RDONLY, 0644);
    fd = open("/etc/hosts",O_RDONLY);
    if(fd < 0) {
        printf("shm_open(%s) failed w/ %s\n", "/etc/hosts", strerror(errno));
        return NULL;
    }

    jstring content;
    char *res;
    char buf[4096];
    int err = 0;

    memset(buf, 0, sizeof(buf));
//    lock_shm(fd, SHMLOCK_RD);
    err = read(fd, buf, 4096);
//    lock_shm(fd, SHMLOCK_UN);
    if (err < 0)
        printf("read in shm file error %s(%d)\n", strerror(errno), errno);
    close(fd);
    res = buf;
out:
    content = env->NewStringUTF(res);
    return content;

}

