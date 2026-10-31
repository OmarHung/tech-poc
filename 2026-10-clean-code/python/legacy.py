# 重構前的原貌：故意保留常見的壞味道，不要照這樣寫。
# 縮寫命名、5 個位置參數、5 層巢狀、魔術數字、重量級距抄了 3 次、用 -1 當錯誤碼、
# 註解只重複程式在做什麼。characterization.py 會拿它當行為的標準答案。


def calc(w, r, m, a, c):
    # 計算運費
    if w > 0 and a >= 0:
        if r == "main":
            if m == "gold":
                if a >= 500:
                    f = 0
                else:
                    if w <= 1000:
                        f = 60
                    elif w <= 5000:
                        f = 100
                    else:
                        f = 100 + ((w - 5000 + 999) // 1000) * 20
            else:
                if a >= 1000:
                    f = 0
                else:
                    if w <= 1000:
                        f = 60
                    elif w <= 5000:
                        f = 100
                    else:
                        f = 100 + ((w - 5000 + 999) // 1000) * 20
            # 加上冷藏費
            if c:
                f = f + 50
        else:
            if w <= 1000:
                f = 60 + 80
            elif w <= 5000:
                f = 100 + 80
            else:
                f = 100 + ((w - 5000 + 999) // 1000) * 20 + 80
            # 加上冷藏費
            if c:
                f = f + 100
        return f
    else:
        return -1
