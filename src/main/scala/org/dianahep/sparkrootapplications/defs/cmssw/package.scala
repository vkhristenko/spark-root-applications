package org.dianahep.sparkrootapplications.defs

package object cmssw {
  /*
   * Common things for cmssw
   */
  case class Empty();
  case class Coordinates(x: Float, y: Float, z: Float);
  case class Vertex(cs: Coordinates);
  case class RecoLeafCandidate(base: Empty, qx3: Int, pt: Float,
                              eta: Float, phi: Float, mass: Float,
                              vertex: Vertex, pdfId: Int, status: Int,
                              cachePolarFixed: Empty, cacheCartesian: Empty);
  case class RecoLeafCandidateStripped(qx3: Int, pt: Float,
                              eta: Float, phi: Float, mass: Float,
                              vertex: Vertex, pdfId: Int, status: Int);
  // companion for our case class
  object RecoLeafCandidateStripped {
    def apply(x: RecoLeafCandidate): RecoLeafCandidateStripped = 
      new RecoLeafCandidateStripped(x.qx3, x.pt, x.eta, x.phi, x.mass, x.vertex, x.pdfId,
        x.status)
  }

  // di-candidate for some combination of 2 things
  case class DiCandidate(e: Float, px: Float, py: Float, pz: Float) {
    val mass = e*e - px*px - py*py - pz*pz;
    val pt = math.sqrt(px*px + py*py)
    val phi = if (px==0) -3.14/2 else math.atan(py/px)
    val eta = 0.5 * math.log((e+pz)/(e-pz))
  }

  val massMuon = 0.10565836727619171;
  def buildDiCandidate(m1: RecoLeafCandidate, m2: RecoLeafCandidate): DiCandidate = {
    val pt1 = math.abs(m1.pt); val pt2 = math.abs(m2.pt);
    val phi1 = m1.phi; val phi2 = m2.phi
    val theta1 = 2.0*math.atan(math.exp(-m1.eta));
    val theta2 = 2.0*math.atan(math.exp(-m2.eta))

    val px1 = pt1 * math.cos(phi1);
    val px2 = pt2 * math.cos(phi2);
    val py1 = pt1 * math.sin(phi1);
    val py2 = pt2 * math.sin(phi2);
    val pz1 = pt1 / math.tan(theta1);
    val pz2 = pt2 / math.tan(theta2);

    val e1 = math.sqrt(px1*px1 + py1*py1 + pz1*pz1 + massMuon*massMuon);
    val e2 = math.sqrt(px2*px2 + py2*py2 + pz2*pz2 + massMuon*massMuon);
    DiCandidate((e1+e2).toFloat,(px1+px2).toFloat, (py1+py2).toFloat, (pz1+pz2).toFloat)
  }
}
