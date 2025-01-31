import test_connect
import test_task
import test_sync

if __name__ == "__main__":
    print("开始全部测试")
    test_connect.main()
    print("-------------------成功-------------------")
    test_task.main()
    print("-------------------成功-------------------")
    test_sync.main()
    print("-------------------成功-------------------")