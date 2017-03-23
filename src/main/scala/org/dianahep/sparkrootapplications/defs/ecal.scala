package org.dianahep.sparkrootapplications.defs

object ecal {
  /*
   * Definitions for CMS ECAL Subsystem in fasion similar to
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/EcalDigi/interface/EcalDataFrame.h
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/EcalDigi/interface/EcalMGPASample.h
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/Common/interface/DataFrameContainer.h
   */
  abstract class DataFrame(val id: Int, val data: Seq[Short], val size: Int);
  case class EcalSample(raw: Short) {
    val adc = raw & 0xFFF;
    val gainId = (raw >> 12) & 0x3;
  }
  case class EcalDataFrame(val id: Int, val data: Seq[EcalSample],
                           val size: Int);
  case class Wrapper(digis: Seq[EcalDataFrame]);

  /*
   * Definitions for CMS ECAL Subsystem as to what actually sits on disk.
   */
  case class DataFrameContainer(m_subdetId: Int, m_stride: Int,
                          m_ids: Seq[Int], m_data: Seq[Short]);

  /*
   * Conversion from what sits on disk (representation of DIGI) to 
   * something that is human-readable
   */
  def convert2Readable(collection: DataFrameContainer): Wrapper = 
      Wrapper(for (i <- 0 until collection.m_ids.length) yield EcalDataFrame(
        collection.m_ids(i), 
        collection.m_data.slice(i*collection.m_stride, (i+1)*collection.m_stride)
          .map(EcalSample(_)),
        collection.m_stride))
}
