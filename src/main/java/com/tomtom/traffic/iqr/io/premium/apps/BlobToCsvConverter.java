package com.tomtom.traffic.iqr.io.premium.apps;

import com.tomtom.traffic.iqr.io.premium.blob.PremiumProfileBlobConverter;
import com.tomtom.traffic.iqr.io.premium.blob.PremiumProfileBlobData;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class BlobToCsvConverter {

  public static void main(String[] argsv) throws IOException {
    PremiumProfileBlobConverter converter = new PremiumProfileBlobConverter();
    File file = new File(argsv[0]);

    DatumReader<GenericRecord> blobDataDatumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, blobDataDatumReader);
    int count = 0;
    System.out.println("NETW_ID,VALIDITY_DIRECTION,timeResolutionMinutes,weekDaySpeed,weekendSpeed,sun,mon,tue,wed,thu,fri,sat");
    while (dataFileReader.hasNext()) {

      GenericRecord next = dataFileReader.next();
      count++;
      Utf8 netwId = (Utf8)next.get("NETW_ID");
      Integer validityDirection = (Integer)next.get("VALIDITY_DIRECTION");
      ByteBuffer profile = (ByteBuffer)next.get("PREMIUM_SPEED_PROFILE");
      PremiumProfileBlobData premiumProfileBlobData = converter.fromBinaryBlob(profile.array());
      StringBuffer sb = new StringBuffer();
      sb.append(netwId)
        .append(",")
        .append(validityDirection)
        .append(",")
        .append(premiumProfileBlobData.getTimeResolutionMinutes())
        .append(",")
        .append(premiumProfileBlobData.getWeekDaySpeed())
        .append(",")
        .append(premiumProfileBlobData.getWeekendSpeed())
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(0)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(1)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(2)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(3)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(4)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(5)))
        .append(",")
        .append(printOptionalArray(premiumProfileBlobData.getDaySpeedsAsArray(6)));


      System.out.println(sb.toString());

    }
  }

  private static String printOptionalArray(Optional<double[]> daySpeedsAsArray) {
    if (Optional.empty().equals(daySpeedsAsArray))
      return "";
    StringBuffer sb = new StringBuffer();
    for (double v : daySpeedsAsArray.get()) {
      sb.append(v);
      sb.append(";");
    }
    return sb.substring(0, sb.length()-1);
  }
}
