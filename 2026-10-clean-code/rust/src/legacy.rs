//! 重構前的原貌：故意保留常見的壞味道，不要照這樣寫。
//! 縮寫命名、5 個位置參數、5 層巢狀、魔術數字、重量級距抄了 3 次、用 -1 當錯誤碼、
//! 註解只重複程式在做什麼。characterization.rs 會拿它當行為的標準答案。

// clippy 預設會抓到其中兩種：多餘的 return、`f = f + 50` 沒寫成 `+=`。開 clippy::pedantic
// 還會再抓到可合併的 `else { if }`、單字母變數。這個檔案就是要把壞味道留下來示範，
// 所以只對 calc() 關掉，不改寫法
#[allow(
    clippy::needless_return,
    clippy::assign_op_pattern,
    reason = "刻意保留的壞味道，見檔案開頭說明"
)]
pub fn calc(w: i64, r: &str, m: &str, a: i64, c: bool) -> i64 {
    // 計算運費
    let mut f;
    if w > 0 && a >= 0 {
        if r == "main" {
            if m == "gold" {
                if a >= 500 {
                    f = 0;
                } else {
                    if w <= 1000 {
                        f = 60;
                    } else if w <= 5000 {
                        f = 100;
                    } else {
                        f = 100 + ((w - 5000 + 999) / 1000) * 20;
                    }
                }
            } else {
                if a >= 1000 {
                    f = 0;
                } else {
                    if w <= 1000 {
                        f = 60;
                    } else if w <= 5000 {
                        f = 100;
                    } else {
                        f = 100 + ((w - 5000 + 999) / 1000) * 20;
                    }
                }
            }
            // 加上冷藏費
            if c {
                f = f + 50;
            }
        } else {
            if w <= 1000 {
                f = 60 + 80;
            } else if w <= 5000 {
                f = 100 + 80;
            } else {
                f = 100 + ((w - 5000 + 999) / 1000) * 20 + 80;
            }
            // 加上冷藏費
            if c {
                f = f + 100;
            }
        }
        return f;
    } else {
        return -1;
    }
}
