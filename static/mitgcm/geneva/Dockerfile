FROM eawag/mitgcm:67z
COPY run_config /simulation/run_config
COPY code /simulation/code
RUN cd /simulation/build && $MITGCM_ROOTDIR/tools/genmake2 -mods ../code -mpi -enable=mnc -of ${MITGCM_ROOTDIR}/tools/build_options/linux_amd64_gfortran && make depend && make
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]