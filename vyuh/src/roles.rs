// roles.rs

use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts},
    http::request::Parts,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fmt::Debug, marker::PhantomData};

use crate::{
    Site,
    auth::{AuthError, AuthUser},
};

// Re-export the BitRole derive macro
pub use vyuh_macros::BitRole;

pub type RoleType = u64;

pub trait BitRole: Sized + Debug + Copy + Clone + 'static {
    /// Returns the bit position (0..=63) for this role.
    ///
    /// Implementations should ensure the value is < 64.
    fn role_value(self) -> u8;

    /// Returns pairs of (bit_position, name) for all roles.
    fn role_pairs() -> &'static [(u8, &'static str)];

    #[inline]
    fn role_name(self) -> Option<&'static str> {
        for (val, name) in Self::role_pairs() {
            if *val == self.role_value() {
                return Some(name);
            }
        }
        None
    }

    #[inline]
    fn to_role_type(self) -> RoleType {
        (1 as RoleType)
            .checked_shl(self.role_value() as u32)
            .unwrap_or(0)
    }
}

pub fn format_roles<R: BitRole>(mask: RoleType) -> Vec<String> {
    let mut roles = Vec::new();
    for (val, name) in R::role_pairs() {
        let Some(role_bit) = (1 as RoleType).checked_shl(*val as u32) else {
            continue;
        };
        if mask & role_bit != 0 {
            roles.push(name.to_string());
        }
    }
    roles
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PermitAny;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PermitAll;

pub struct Permit<const MASK: RoleType, R: BitRole, O = PermitAny>(
    pub AuthUser,
    pub PhantomData<R>,
    pub PhantomData<O>,
);

impl<const MASK: RoleType, R: BitRole> Permit<MASK, R> {
    pub fn into_user(self) -> AuthUser {
        self.0
    }
}

pub trait HasPerm {
    fn has_permission(role_mask: RoleType, perm_mask: RoleType) -> bool;

    fn join_all() -> bool {
        false
    }
}

impl HasPerm for PermitAny {
    fn has_permission(role_mask: RoleType, perm_mask: RoleType) -> bool {
        role_mask & perm_mask != 0
    }
}

impl HasPerm for PermitAll {
    fn has_permission(role_mask: RoleType, perm_mask: RoleType) -> bool {
        role_mask & perm_mask == perm_mask
    }

    fn join_all() -> bool {
        true
    }
}

impl<const MASK: RoleType, R: BitRole, O: HasPerm> FromRequestParts<Site> for Permit<MASK, R, O> {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        let user = <AuthUser as FromRequestParts<Site>>::from_request_parts(parts, site).await?;
        if !O::has_permission(user.roles, MASK) {
            return Err(AuthError::Forbidden);
        }
        Ok(Permit(user, PhantomData, PhantomData))
    }
}

impl<const MASK: RoleType, R: BitRole, O: HasPerm> OptionalFromRequestParts<Site>
    for Permit<MASK, R, O>
{
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        site: &Site,
    ) -> Result<Option<Self>, Self::Rejection> {
        match <Permit<MASK, R, O> as FromRequestParts<Site>>::from_request_parts(parts, site).await
        {
            Ok(permit) => Ok(Some(permit)),
            Err(AuthError::NoCredential | AuthError::Forbidden) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<const MASK: RoleType, R: BitRole, O: HasPerm> crate::callables::IntoArgPart
    for Permit<MASK, R, O>
{
    fn into_arg_part() -> crate::callables::ArgPart {
        let scopes = R::role_pairs()
            .iter()
            .filter_map(|(bit, name)| {
                let role_bit = (1 as RoleType).checked_shl(*bit as u32)?;
                (MASK & role_bit != 0).then(|| Cow::Borrowed(*name))
            })
            .collect();
        crate::callables::ArgPart::Composite(vec![
            crate::callables::ArgPart::Authentication,
            crate::callables::ArgPart::Security {
                scheme: Cow::Borrowed("vyuhAuth"),
                scopes,
                join_all: O::join_all(),
            },
            crate::callables::ArgPart::Response(vec![
                crate::callables::ReturnSpec::error(401, "Unauthorized."),
                crate::callables::ReturnSpec::error(403, "Forbidden."),
            ]),
        ])
    }
}

#[macro_export]
macro_rules! permit {
    // Internal helper: role position -> mask
    (@mask $role_ty:ty, $role:ident) => {
        (<$role_ty>::__vyuh_mask(<$role_ty>::$role))
    };

    // permit!(RoleType, Role) - single role, defaults to PermitAny
    ($role_ty:ty, $role:ident $(,)?) => {
        $crate::auth::Permit::<{
            $crate::permit!(@mask $role_ty, $role)
        }, $role_ty, $crate::auth::PermitAny>
    };

    // permit!(RoleType, Role1 & Role2 & Role3) - ALL required (PermitAll)
    ($role_ty:ty, $first:ident $( & $rest:ident )+ $(,)?) => {
        $crate::auth::Permit::<{
            $crate::permit!(@mask $role_ty, $first)
            $( | $crate::permit!(@mask $role_ty, $rest) )+
        }, $role_ty, $crate::auth::PermitAll>
    };

    // permit!(RoleType, Role1 | Role2 | Role3) - ANY required (PermitAny)
    ($role_ty:ty, $first:ident $( | $rest:ident )+ $(,)?) => {
        $crate::auth::Permit::<{
            $crate::permit!(@mask $role_ty, $first)
            $( | $crate::permit!(@mask $role_ty, $rest) )+
        }, $role_ty, $crate::auth::PermitAny>
    };
}
