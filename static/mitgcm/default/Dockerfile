FROM eawag/mitgcm:67z
COPY code /simulation/code
RUN cd /simulation/build && /mitgcm/tools/genmake2 -mods ../code -mpi -enable=mnc -of /mitgcm/tools/build_options/linux_amd64_gfortran && make depend && make
RUN useradd -ms /bin/bash appuser
USER appuser
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]