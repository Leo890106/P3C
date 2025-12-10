# P3C-tree: Partial Pre-Post Coding Tree

> A memory-efficient variant of PPC-tree for fast N-list construction and support counting.

本專案實作 **P3C-tree (Partial Pre-Post Coding tree)**，用來在大規模資料集上，節省記憶體地建構 **N-lists**，並支援快速計算任意 selector conjunction（類似 frequent itemset）的 support。

---

## 1. 背景介紹

在關聯規則 / 項目集挖掘與規則分類問題中，我們常需要大量查詢：

> 某一組條件（selectors）同時出現的次數是多少？

常見做法是：

1. 先把交易資料建成一棵 **PPC-tree (Pre-Post Coding tree)**  
2. 再從 PPC-tree 轉成 **N-lists**  
3. 任意 k-selector set 的 support 就透過 N-lists 的交集計算出來  

問題是：

- **PPC-tree 在大資料集上非常吃記憶體**
- PPC-tree 本身只是中間產物，建完後還要再轉成 N-lists，整個過程對 RAM 不友善  

為了解決這個問題，論文提出 **P3C-tree**：

> 在保證生成的 N-lists 與原始 PPC-tree 完全相同的前提下，大幅降低建樹時的記憶體用量，同時縮短執行時間。

---

## 2. P3C-tree 核心概念

P3C-tree 把整棵 PPC-tree 拆成兩部分：

1. **Static Top Part（靜態上半部）**
   - 只保留樹的上半部（較高層的節點），作為「靜態樹幹」
   - 這部分會一直存在記憶體中

2. **Transient Subtrees（暫時性葉子子樹）**
   - 對 static tree 的每一個葉節點 (leaf)，只針對「經過這個 leaf 的交易」建立一棵暫時性子樹
   - 立刻用這棵子樹更新對應 selectors 的 **basic N-lists**
   - 用完即釋放（free 掉整棵子樹），記憶體在整個過程中反覆回收

換句話說：

> 在任意時間點，記憶體中只會有「一棵 top-part + 一棵 leaf 子樹」，不需要把完整 PPC-tree 全部保留在 RAM 裡。

理論與實驗顯示：

- 透過分段建樹 + 立刻轉 N-lists 的流程  
- **最終得到的 basic N-lists 與傳統 PPC-tree 方法完全一致**（準確且完整）

---

## 3. 重要名詞說明

- **Selector（選擇器）**  
  一般形式類似 `(A_i = a_ij)` 的條件，一個 selector 可以視為一個「項目」，一筆交易是一組 selectors 的集合。

- **PPC-tree node**  
  每個節點代表一個 selector，並記錄：
  - 該 selector 出現次數 `freq`
  - pre / post traversal 編碼 `⟨pre, post⟩`，用來判斷 node 之間的祖先 / 子孫關係

- **N-list**  
  對於每個 selector `x`，收集 PPC-tree 上所有 label 為 `x` 的節點，形成：

  ```text
  NList(x) = { ⟨pre, post⟩ : freq } 的集合

之後任意 k-selector set 的 support，都透過這些 N-lists 的「ancestor-descendant intersection」計算。

---

## 4. 參數 `e`（Efficiency Factor）

P3C-tree 有一個關鍵參數 `e`，用來決定 **靜態上半樹 (top-part)** 的「深度 / 大小」。

先計算：

    max_freq = |T| / e

其中 `|T|` 是交易數量。

在建 static top tree 時：

- 若節點 `freq > max_freq`，則繼續往下分裂（長出下一層子節點）
- 當所有 leaf 節點的 `freq <= max_freq` 時，停止分裂，得到一棵 top-part

**直覺理解：**

- `e = 1`  
  - `max_freq = |T|`  
  - 只有 root 的 `freq` 會 ≥ `|T|`，其他節點不再分裂  
  - 等於 top-part 只有 root，後面建出的 subtree 就會變成完整 PPC-tree  
  - `e = 1` 幾乎就是原始 PPC-tree 的情況  

- `e` 變大  
  - `max_freq` 變小 → top-part 越深、節點越多  
  - 每個 leaf 底下的 transient subtree 變小（因為 leaf 代表的交易數變少）  

**實務上的觀察：**

- 太小的 `e`：top-part 太小，leaf subtree 太大 → 記憶體壓力大
- 太大的 `e`：top-part 本身就變得很肥，也會吃掉大量 RAM
- 在多個大型資料集上：`e ≈ 500 ~ 1500` 之間，記憶體與 runtime 都接近最佳

一個簡單且實用的預設值：

    e = 1000

---

## 5. 演算法流程概述

### 5.1 預處理

- 掃描資料集一次，計算每個 selector 的出現次數  
- 根據頻率（通常由小到大）排序 selectors，得到全域順序 `O`  
- 將每筆交易中的 selectors 依照 `O` 重新排序  

### 5.2 建立 Static Top Tree

從 root 開始，持有所有交易。

對每一層：

- 根據交易中當前位置的 selector，分配到對應子節點  
- 每個節點維護：
  - `freq`
  - `tlist`（經過該節點的交易列表）
- 若 `freq > max_freq`，則節點繼續分裂下一層；否則成為 leaf  
- 每層處理完即可清掉暫存的 `tlist`，節省記憶體  

### 5.3 針對每個 Leaf 建立 Transient Subtree

對每一個 leaf 節點：

1. 收集所有經過該 leaf 的交易，將交易中已在上層使用過的 selectors 剪掉，只保留剩餘部分  
2. 使用這些剩餘 selectors 建一棵以該 leaf 為根的 subtree  
3. 在「ancestor + subtree」上進行 pre/post traversal，為所有新節點賦予 `⟨pre, post⟩`  
4. 把節點的 `⟨pre, post⟩ : freq` 加入對應 selector 的 basic N-list  
5. 完成後釋放整棵 subtree 的記憶體  

重複上述步驟直到所有 leaf 都處理完，即完成所有 selectors 的 N-lists 建構。

---

## 6. 環境需求與建置方式

- JDK 8+  
- eclipse 下載後直接導入 
- 記憶體至少 8 GB（大型資料集建議 16 GB+）  
-如果是JVM out of memory 使用 -Xms16g -Xmx16g 指令

## 7. 使用方式

   -到 https://www.kaggle.com/competitions/avazu-ctr-prediction/data 下載我們實驗的資料集
   -將要實驗的資料集檔案放到 data/input/ 底下
   -執行zbenchmark底下的 InfoBaseBenchmarkMemoryPPCTree 以及 InfoBaseBenchmarkMemoryP3CTree 
   -到output資料夾看執行結果


 
