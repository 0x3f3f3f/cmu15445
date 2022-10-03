#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; }

/**
 * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
 *
 * In Extendible Hashing we map a key to a directory index
 * using the following hash + mask function.
 *
 * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
 *
 * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
 * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
 * representation.
 *
 * @return mask of global_depth 1's and the rest 0's (with 1's from LSB upwards)
 */
uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
  // 这里根据注释中的示例写就行
  // Example: 当global_depth_是3的时候
  // 0000...000001 << global_depeth_ = 0000...01000
  // 再减1即可
  return ((1 << global_depth_) - 1);
}

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
  uint8_t depth = local_depths_[bucket_idx];
  return (1 << depth) - 1;
}

/**
 * Increment the global depth of the directory
 */
void HashTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ < MAX_BUCKET_DEPTH);
  // 这里主要是需要将bucket_page_ids_和local_depths_的数据在现有数组的末尾再复制一份
  // 扩容时没有进行数据迁移，两个bucket对应一个page
  int org_num = Size();
  for (int org_index = 0, new_index = org_num; org_index < org_num; new_index++, org_index++) {
    bucket_page_ids_[new_index] = bucket_page_ids_[org_index];
    local_depths_[new_index] = local_depths_[org_index];
  }
  global_depth_++;
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

/**
 * Lookup a bucket page using a directory index
 *
 * @param bucket_idx the index in the directory to lookup
 * @return bucket page_id corresponding to bucket_idx
 */
page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { return bucket_page_ids_[bucket_idx]; }

/**
 * Updates the directory index using a bucket index and page_id
 *
 * @param bucket_idx directory index at which to insert page_id
 * @param bucket_page_id page_id to insert
 */
void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

/**
 * @return the current directory size
 */
uint32_t HashTableDirectoryPage::Size() {
  // 2 ^ global_depth_
  return (1 << global_depth_);
}

/**
 * @return true if the directory can be shrunk
 */
bool HashTableDirectoryPage::CanShrink() {
  // 整个Directory能不能收缩取决于每个localdepth是否都比globaldepth小
  // 循环判断即可
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

/**
 * Gets the local depth of the bucket at bucket_idx
 *
 * @param bucket_idx the bucket index to lookup
 * @return the local depth of the bucket at bucket_idx
 */
uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { return local_depths_[bucket_idx]; }

/**
 * Set the local depth of the bucket at bucket_idx to local_depth
 *
 * @param bucket_idx bucket index to update
 * @param local_depth new local depth
 */
void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(local_depth <= global_depth_);
  local_depths_[bucket_idx] = local_depth;
}

/**
 * Increment the local depth of the bucket at bucket_idx
 * @param bucket_idx bucket index to increment
 */
void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(local_depths_[bucket_idx] < global_depth_);
  local_depths_[bucket_idx]++;
}

/**
 * Decrement the local depth of the bucket at bucket_idx
 * @param bucket_idx bucket index to decrement
 */
void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

/**
 * Gets the split image of an index
 * SplitImage可以理解为兄弟bucket
 *
 * @param bucket_idx the directory index for which to find the split image
 * @return the directory index of the split image
 **/
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  // Example: 当对应的localdepth是3时：
  // 1<<(3-1) = 0000....00100
  // 具体用途后面再说
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
