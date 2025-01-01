/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>

#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"
#include "velox/type/HugeInt.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::parquet {

class TimestampDecoder : public dwio::common::DirectDecoder<true> {
 public:
  explicit TimestampDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      const thrift::TimeUnit& unit,
      bool useVInts,
      bool bigEndian = false)
      : DirectDecoder(std::move(input), useVInts, 8, bigEndian) {
    if (unit.__isset.MILLIS) {
      timestampAdjustment_ = Timestamp::kMillisecondsInSecond;
    } else if (unit.__isset.MICROS) {
      timestampAdjustment_ = Timestamp::kMicrosecondsInMillisecond *
          Timestamp::kMillisecondsInSecond;
    } else if (unit.__isset.NANOS) {
      timestampAdjustment_ = Timestamp::kNanosInSecond;
    } else {
      std::stringstream oss;
      unit.printTo(oss);
      VELOX_USER_FAIL(
          "Unsupported timestamp unit for plain encoding: {}", oss.str());
    }
  }

  int128_t readTimestamp() {
    auto int64_ts = this->template readInt<int64_t>();
    auto ts = Timestamp::from(int64_ts, timestampAdjustment_);
    return *reinterpret_cast<int128_t*>(&ts);
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    int32_t current = visitor.start();
    this->template skip<hasNulls>(current, 0, nulls);
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      bool atEnd = false;
      int32_t toSkip;
      if (hasNulls) {
        if (!allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            this->template skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        } else {
          if (bits::isBitNull(nulls, current)) {
            toSkip = visitor.processNull(atEnd);
            goto skip;
          }
        }
      }

      // We are at a non-null value on a row to visit.
      toSkip = visitor.process(readTimestamp(), atEnd);
    skip:
      ++current;
      if (toSkip) {
        this->template skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 private:
  int timestampAdjustment_{1};
};

} // namespace facebook::velox::parquet
