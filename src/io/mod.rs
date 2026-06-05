//! I/O utilities: JSON streaming ([`reader`]), terminal progress ([`progress`]),
//! IHM progress events ([`progress_event`]), and memory pressure detection ([`mem`]).
pub mod mem;
pub mod progress;
pub mod progress_event;
pub mod reader;
