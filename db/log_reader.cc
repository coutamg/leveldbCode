// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//参考[https://zhuanlan.zhihu.com/p/35188065]

#include "db/log_reader.h"

#include <stdio.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

//跳转到initial_offset_所在的block的开始位置，同时调整end_of_buffer_offset_，与file_文件指针指向该block的起始地址
bool Reader::SkipToInitialBlock() {
  // 块中偏移，离block开头的多远
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  // 需要跳过的块的位置
  // 这个变量的意思是说，后面在读的时候，要读的块的开头地址是什么，定位到读取block的开始位置
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  // 如果给定的初始位置的块中偏移
  // 刚好掉在了尾巴上的6个bytes以内。那么
  // 这个时候，应该是需要直接切入到下一个block的。
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }
  // 注意end_of_buffer_offset的设置是块的开始地址。，默认缓冲区是按32k增长的，且假设这个是随意申请
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    //这里实际上是把文件指针(设为current)移动到与end_of_buffer_offset_齐平，这里没与initial_offset_对齐(有可能)
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
//这里调到了符合要求的initial_offset_ 的位置
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  /*
    当文件中的record是<firstRecord, middleRecord, lastRecord>的时候。
    scratch需要做一个缓冲区，把一个一个record的数据缓存起来。最后拼接成一个大的 
    Slice返回给客户端。
  */
  scratch->clear();//用于拼接某个record位于多个block上
  record->clear();
  bool in_fragmented_record = false;//true表示在读某个record的中间部分，读取了record的开头部分
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

/*
下面while循环要解决的是：
1. initial_offset_并不是block的整数倍。虽然跳过了block=32KB的整数倍。但是这个余数
应该如何处理？后面还有别的什么作用没有？
2. skip的时候，跳过的都是block的整数倍。但是有可能存在这种较大的Slice。一个slice就是
   N个block。有可能跳过block之后，还是处在slice的中间。这个时候，也是读不了一个完整
   的slice数据。那么，如何跳动，保证后面读一个完整的slice数据？
解决问题2大体逻辑是：读入一个record，如果发现类型是kMiddleType。那么就跳过这个record。
*/
  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);//实际上读的是block里面的内容

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    /*
      1. record_type为kBadRecord，a. buffer_.size == 0 且 fragment.size == 0，一种是head里面的length为0且type == kZeroType
                                     另外一种就是head里面的length大于该block数据的长度(这种情况说明记录block出错了，因为head里的length<=block.size)
                                  b. 只有fragment.size == 0，则表示读到的PhysicalRecord是位于initial_offset_之前的
                                  上述的都是需要跳过去的
    */
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) { // 构造时： resyncing_ = （initial_offset > 0），表示是否要跳过
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        // 文件都读结束了，还处在“读在中间”状态。说明写入的时候没有写入一个完整的
        // record。没办法，直接向客户端返回没有完整的slice数据了。
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        // 如果读到了坏的record，又刚好处理“读在中间”的状态。那么返回出错!!
        // 如果这个坏掉的record不是在读的record范围里面。直接返回读失败。
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

//写入的时候是按照32KB一个block来写入。在读取的时候，就可以32KB来读入了。所以在读入的时候，肯定是以32KB为单位来读的
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {//一般来说，初始化的时候 buffer_ size为0
      if (!eof_) {//这个block没有结束
        // Last read was a full read, so this is a trailer to skip
        // 如果还没有遇到结束
        // 上一次的读是一个完整的读。那么可能这里有一点尾巴需要处理。
        buffer_.clear();
        // 这里是读kBlockSize个字符串到buffer_里面。
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();//先把偏移处理了
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      } else {
        // 注意：如果buffer_是非空的。我们有一个truncated header在文件的尾巴。
        // 这可能是由于在写header时crash导致的。
        // 与其把这个失败的写入当成错误来处理，还不如直接当成EOF呢。
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data(); //由于一个block的开头存的是某个slice的head(不一定是这个slice最初的head，而是这个slice数据在这个block中的head)
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;//length的低8位
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;//length的高8位
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);//这里的length就是写入record里面data的长度，并不包括header
    if (kHeaderSize + length > buffer_.size()) {// 如果头部记录的数据长度比实际的buffer_.size还要大。那肯定是出错了。
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        //这里上报的原因是，sst文件没有读完，后面还写了数据，说明本次 record写入是有问题的
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      //这种情况可能是 write正确的写入了head 但是在写入record的data时down掉了，data没写完，因为sst文件已经读到结尾了
      return kEof;
    }

    // 如果是zero type。那么返回Bad Record
    // 这种情况是有可能的。比如写入record到block里面之后。可能会遇到
    // 还余下7个bytes的情况。这个时候只能写入一个空的record。
    if (type == kZeroType && length == 0) {//kZeroType是留给预分配用的
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 移除当前的record占用的缓冲区，移除的就是位于block其起始的head里面记录的record的长度
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_，这里表示这个物理的record(也就是slicd在某个block上的数据)，位于initial_offset_
    //之前的，这样的record数据是不能要的，也说明了initial_offset_落在某个slice数据中间
    /*  skip的时候，跳过的都是block的整数倍。但是有可能存在这种较大的Slice。一个slice就是
        N个block。有可能跳过block之后，还是处在slice的中间。这个时候，也是读不了一个完整
        的slice数据
    */
    // f->read()..之后有end_of_buffer_offset_ += buffer_.size();
    // 但是这里end_of_buffer_offset_ - buffer_.size()
    // 减了之后，减去的不是刚读出来的数据块的大小。比如32KB。
    // 这个时候的buffer_size.指的是未读的record的数据大小。
    // end_of_buffer_offset_ - buffer_.size就是已经读掉的缓冲区的指针的位置。
    // end_of_buffer_offset_ 
    //      - buffer_.size()
    //     - kHeaderSize
    //     - length
    // 这里得到的就是刚读出来的record的起始位置，其实就是该block的起始位置
    // 这里需要跳过那些起始位置位于initial_offset_之前的record数据
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);//从block里面读到的record
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
