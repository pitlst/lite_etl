import test_connect
import test_task
import test_sync_total

if __name__ == "__main__":
    print("开始全部测试")
    test_connect.main()
    print("-------------------成功-------------------")
    test_task.main()
    print("-------------------成功-------------------")
    test_sync_total.main()
    print("-------------------成功-------------------")