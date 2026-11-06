use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierIo};
pub(crate) struct NativeTcpPreparation(
    zero_platform_tokio::controlled_carrier::CarrierDriver<super::Prepared>,
);
impl ClientConnection {
    pub(crate) async fn prepare_tcp_controlled(
        io: CarrierIo,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<NativeTcpPreparation> {
        io.prepare_driver(|io| async move {
            let (io, source) = super::super::native_io::Capture::new(io);
            prepare(io, username, password, options)
                .await
                .map_err(|error| source.recover(error))
        })
        .await
        .map(NativeTcpPreparation)
    }
    pub(crate) async fn tcp_controlled(
        io: CarrierIo,
        adoption: CarrierAdoption,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: super::super::pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        let owner = lease.owner.clone();
        let attempt_cancel = lease.cancellation();
        let prepared = tokio::select! {biased;
            _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            _=owner.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            result=Self::prepare_tcp_controlled(io,username,password,options)=>result?,
        };
        Self::start_tcp_controlled(prepared, adoption, lease)
    }
    pub(crate) fn start_tcp_controlled(
        prepared: NativeTcpPreparation,
        adoption: CarrierAdoption,
        lease: super::super::pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        use zero_platform_tokio::contract::{
            lifecycle::TaskClass, ChannelError, Impact, Reason, Stage,
        };
        let owner = lease.owner.clone();
        if !owner.ready() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let ((first, opens), prepared) = prepared
            .0
            .split(|prepared| ((prepared.first, prepared.opens), prepared.driver));
        let driver = super::super::driver::ManagedDriver::new(lease.cancellation());
        let completion = driver.completion();
        let stop = driver.cancel.clone();
        let scope_stop = owner.scope_cancel.clone();
        let component = owner.instance.component;
        let registration = lease.registering();
        let (_,witness)=adoption.spawn_driver(&owner.tasks,TaskClass::Critical,prepared,|driver|async move {
            let _lease=lease;let _completion=completion;
            tokio::select! {biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=driver=>result.map_err(|source| {
                    let mut error=ChannelError::new(Stage::Receive,Reason::Io,Impact::Component).with_source(source);
                    error.component=Some(component);error
                }),
            }
        }).map_err(io::Error::other)?;
        driver.registered(witness.clone());
        driver
            .attach_original(&adoption)
            .map_err(io::Error::other)?;
        registration.registered(witness);
        Ok(Self::from_prepared(
            first,
            opens,
            DriverHandle::Managed(driver),
        ))
    }
    #[cfg(test)]
    pub(crate) async fn tcp_managed<S>(
        socket: S,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: super::super::pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        use zero_platform_tokio::contract::{
            lifecycle::TaskClass, ChannelError, Impact, Reason, Stage,
        };
        let owner = lease.owner.clone();
        let attempt_cancel = lease.cancellation();
        let (socket, source) = super::super::native_io::Capture::new(socket);
        let prepared = tokio::select! {biased;
            _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            _=owner.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            result=prepare(socket, username, password, options)=>result.map_err(|error|source.recover(error))?,
        };
        if !owner.ready() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let driver = super::super::driver::ManagedDriver::new(lease.cancellation());
        let completion = driver.completion();
        let stop = driver.cancel.clone();
        let scope_stop = owner.scope_cancel.clone();
        let component = owner.instance.component;
        let registration = lease.registering();
        let (_, witness)=owner.tasks.spawn_tracked(TaskClass::Critical,async move {
            let _lease=lease;
            let _completion=completion;
            tokio::select! {biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=prepared.driver=>result.map_err(|source|{
                    let mut error=ChannelError::new(Stage::Receive,Reason::Io,Impact::Component).with_source(source);
                    error.component=Some(component); error
                }),
            }
        }).map_err(io::Error::other)?;
        driver.registered(witness.clone());
        registration.registered(witness);
        Ok(Self::from_prepared(
            prepared.first,
            prepared.opens,
            DriverHandle::Managed(driver),
        ))
    }
}
