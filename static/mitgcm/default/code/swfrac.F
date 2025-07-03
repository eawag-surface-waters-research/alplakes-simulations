#include "PACKAGES_CONFIG.h"
#include "CPP_OPTIONS.h"

CBOP
C     !ROUTINE: SWFRAC
C     !INTERFACE:
      SUBROUTINE SWFRAC(
     I                  imax, fact,
     U                  swdk,
     I                  myTime, myIter, myThid )
C     !DESCRIPTION: \bv
C     *==========================================================*
C     | SUBROUTINE SWFRAC
C     | o Compute solar short-wave flux penetration.
C     *==========================================================*
C     | Compute fraction of solar short-wave flux penetrating to
C     | specified depth, swdk, due to exponential decay in
C     | Jerlov water type jwtype.
C     | Reference : Two band solar absorption model of Paulson
C     |             and Simpson (1977, JPO, 7, 952-956)
C     | Notes
C     | =====
C     | Parameter jwtype is hardcoded to 2 for time being.
C     | Below 200m the solar penetration gets set to zero,
C     | otherwise the limit for the exponent (+/- 5678) needs to
C     | be taken care of.
C     | Written by   : Jan Morzel
C     | Date         : July 12, 1995
C     | Included modification to adadpt it to lakes in Sept 2018 (Cintia)
C     *==========================================================*
C     \ev

C     !USES:
      IMPLICIT NONE

C     !INPUT/OUTPUT PARAMETERS:
C     === Routine arguments ===
C     input arguments
C     imax    :: number of vertical grid points
C     fact    :: scale  factor to apply to depth array
C     myTime  :: Current time in simulation
C     myIter  :: Current iteration number in simulation
C     myThid  :: My Thread Id. number
      INTEGER imax
      _RL     fact
      _RL     myTime
      INTEGER myIter
      INTEGER myThid
C     input/output arguments
C     swdk    :: on input: vertical depth for desired sw fraction
C               (fact*swdk) is negative distance (m) from surface
C     swdk    :: on output: short wave (radiation) fractional decay
      _RL     swdk(imax)

C     !LOCAL VARIABLES:
C     === Local variables ===
C     max number of different water types
      INTEGER nwtype, secchi
      PARAMETER(nwtype=5)
      _RL facz
      _RL eta,zA,BetaSol
      INTEGER i
      _RL     fac
      LOGICAL first, changed
      INTEGER count0, count1
      !depths!
      !secchi!

#ifdef ALLOW_CAL
      CALL  cal_GetMonthsRec(
     O     fac, first, changed, count0, count1,
     I     myTime, myIter, myThid )
           secchi=secchiDepths(count1)
#else /* ALLOW_CAL undef */
      secchi=4.5 _d 0
#endif /* ALLOW_CAL */

      DO i = 1,imax
         facz = fact*swdk(i)
         IF ( abs(facz) .LT. -100. _d 0 ) THEN
            swdk(i) = 0. _d 0
         ELSE
            swdk(i) = exp( 1.7 * facz / secchi)
		ENDIF
      ENDDO

      RETURN
      END
