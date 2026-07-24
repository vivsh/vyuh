//! Tests for bundlepart infrastructure with darling-based parsing.
//!
//! Tests the architecture:
//! 1. Parse function signature → FnSpec (with pos in args)
//! 2. Parse overrides with darling → apply to FnSpec
//! 3. Build patches from FnSpec

use super::*;
use proc_macro2::TokenStream;
use quote::quote;

// ============================================================================
// Signature parsing tests
// ============================================================================

#[test]
fn test_extract_simple_function() {
    let input: TokenStream = quote! {
        fn handler(id: UserId, name: String) -> Result<User> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert_eq!(spec.name, "handler");
    assert_eq!(spec.args.len(), 2);
    assert_eq!(spec.args[0].pos, 0);
    assert_eq!(spec.args[0].name, "id");
    assert_eq!(spec.args[0].ty, None);
    assert_eq!(spec.args[1].pos, 1);
    assert_eq!(spec.args[1].name, "name");
    assert!(
        spec.returns.is_empty(),
        "Expected at 0 return type: {:#?}",
        spec.returns
    );
    assert!(!spec.method);
}

#[test]
fn test_extract_method_with_self() {
    let input: TokenStream = quote! {
        fn process(&self, count: usize) -> Vec<Item> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert_eq!(spec.name, "process");
    assert!(spec.method);
    assert!(spec.receiver.is_some());
    // Self is in receiver, args only has non-self params
    assert_eq!(spec.args.len(), 1);
    assert_eq!(spec.args[0].pos, 0);
    assert_eq!(spec.args[0].name, "count");
}

#[test]
fn test_extract_with_doc_comments() {
    let input: TokenStream = quote! {
        /// Process user data
        /// Returns the updated user
        fn handler(id: UserId) -> User {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.description.is_some());
    let desc = spec.description.unwrap();
    assert!(desc.contains("Process"));
    assert!(desc.contains("updated"));
}

#[test]
fn test_extract_with_markdown_doc_comments() {
    let input: TokenStream = quote! {
        /// # Handler Function
        ///
        /// This function processes **user data** with the following features:
        ///
        /// - Item 1
        /// - Item 2
        ///
        /// ```rust
        /// let x = 42;
        /// ```
        ///
        /// Returns `User` or *error*.
        fn handler(id: UserId) -> User {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.description.is_some());
    let desc = spec.description.unwrap();

    // Check markdown formatting is preserved
    assert!(desc.contains("# Handler Function"));
    assert!(desc.contains("**user data**"));
    assert!(desc.contains("- Item 1"));
    assert!(desc.contains("- Item 2"));
    assert!(desc.contains("```rust"));
    assert!(desc.contains("let x = 42;"));
    assert!(desc.contains("```"));
    assert!(desc.contains("`User`"));
    assert!(desc.contains("*error*"));
}

#[test]
fn test_doc_comments_preserve_empty_lines() {
    let input: TokenStream = quote! {
        /// First paragraph with content.
        ///
        /// Second paragraph after empty line.
        ///
        /// Third paragraph.
        fn handler(id: UserId) -> User {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.description.is_some());
    let desc = spec.description.unwrap();

    // Empty lines should be preserved for proper markdown formatting
    assert!(desc.contains("First paragraph with content.\n\nSecond paragraph"));
    assert!(desc.contains("Second paragraph after empty line.\n\nThird paragraph"));
}

#[test]
fn test_doc_comments_preserve_indentation() {
    let input: TokenStream = quote! {
        /// Code example:
        ///
        ///     let x = 42;
        ///     let y = 100;
        ///
        /// End of example.
        fn handler() {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.description.is_some());
    let desc = spec.description.unwrap();

    // Leading spaces should be preserved for indented code blocks
    assert!(desc.contains("    let x = 42;"));
    assert!(desc.contains("    let y = 100;"));
}

// ============================================================================
// Receiver parsing tests
// ============================================================================

#[test]
fn test_receiver_ref_self() {
    let input: TokenStream = quote! {
        fn process(&self, data: String) -> Result<()> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    if let Some(ReceiverSpec::Ref { mut_ref }) = spec.receiver {
        assert!(!mut_ref);
    } else {
        panic!("Expected ReceiverSpec::Ref with mut_ref=false");
    }
}

#[test]
fn test_receiver_mut_ref_self() {
    let input: TokenStream = quote! {
        fn modify(&mut self, value: i32) -> () {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    if let Some(ReceiverSpec::Ref { mut_ref }) = spec.receiver {
        assert!(mut_ref);
    } else {
        panic!("Expected ReceiverSpec::Ref with mut_ref=true");
    }
}

#[test]
fn test_receiver_value_self() {
    let input: TokenStream = quote! {
        fn consume(self) -> Result<Data> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    if let Some(ReceiverSpec::Value { mut_self }) = spec.receiver {
        assert!(!mut_self);
    } else {
        panic!("Expected ReceiverSpec::Value with mut_self=false");
    }
}

#[test]
fn test_receiver_mut_value_self() {
    let input: TokenStream = quote! {
        fn consume_mut(mut self) -> Result<Data> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    if let Some(ReceiverSpec::Value { mut_self }) = spec.receiver {
        assert!(mut_self);
    } else {
        panic!("Expected ReceiverSpec::Value with mut_self=true");
    }
}

#[test]
fn test_receiver_typed_self() {
    let input: TokenStream = quote! {
        fn custom(self: Box<Self>) -> Result<()> {}
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    if let Some(ReceiverSpec::Typed { ty }) = spec.receiver {
        assert!(ty.contains("Box"));
        assert!(ty.contains("Self"));
    } else {
        panic!("Expected ReceiverSpec::Typed");
    }
}

// ============================================================================
// Method detection in impl blocks
// ============================================================================

#[test]
fn test_method_in_impl_block() {
    let input: TokenStream = quote! {
        fn process(&self, count: usize) -> Vec<Item> {
            vec![]
        }
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    // Even standalone functions with &self are detected as methods
    assert!(spec.method);
    assert!(spec.receiver.is_some());
}

#[test]
fn test_function_without_receiver() {
    let input: TokenStream = quote! {
        fn standalone(count: usize) -> Vec<Item> {
            vec![]
        }
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(!spec.method);
    assert!(spec.receiver.is_none());
}

#[test]
fn test_impl_item_method() {
    // Test parsing ImplItemFn (method inside impl block)
    let input: TokenStream = quote! {
        pub fn process(&mut self, data: String) -> Result<()> {
            Ok(())
        }
    };

    let spec = extract_func_spec(&input, "test").unwrap();

    assert!(spec.method);
    assert!(spec.receiver.is_some());
    assert_eq!(spec.args.len(), 1);
    assert_eq!(spec.args[0].name, "data");
}

// ============================================================================
// Override parsing and application tests
// ============================================================================

#[test]
fn test_parse_and_apply_arg_override_by_name() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        arg(name = "id", ty = "i64", description = "User identifier")
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![FnArg {
            pos: 0,
            name: "id".to_string(),
            ty: Some(syn::parse_str("UserId").unwrap()),
            description: None,
        }],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.args[0].ty, Some(syn::parse_str("i64").unwrap()));
    assert_eq!(spec.args[0].description.as_deref(), Some("User identifier"));
}

#[test]
fn test_parse_and_apply_arg_override_by_position() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        arg(pos = 0, name = "id", ty = "i64")
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![FnArg {
            pos: 0,
            name: "id".to_string(),
            ty: Some(syn::parse_str("UserId").unwrap()),
            description: None,
        }],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.args[0].ty, Some(syn::parse_str("i64").unwrap()));
}

#[test]
fn test_parse_and_apply_returns() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        returns(ty = "ErrorResponse", status = 404, description = "Not found")
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.returns.len(), 1);
    assert_eq!(
        spec.returns[0].ty,
        Some(syn::parse_str("ErrorResponse").unwrap())
    );
    assert_eq!(spec.returns[0].status, Some(404));
    assert_eq!(spec.returns[0].description.as_deref(), Some("Not found"));
}

#[test]
fn test_parse_and_apply_signature_return_status_override() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        returns(status = 201, description = "Created")
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.returns.len(), 1);
    assert!(spec.returns[0].ty.is_none());
    assert_eq!(spec.returns[0].status, Some(201));
    assert_eq!(spec.returns[0].description.as_deref(), Some("Created"));
}

#[test]
fn test_parse_and_apply_description() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        description = "Custom handler"
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.description.as_deref(), Some("Custom handler"));
}

#[test]
fn test_parse_and_apply_mixed() {
    let items: Vec<darling::ast::NestedMeta> = darling::ast::NestedMeta::parse_meta_list(quote! {
        description = "Mixed test",
        arg(name = "id", ty = "i64"),
        returns(ty = "User", status = 200),
        returns(ty = "ErrorResponse", status = 404)
    })
    .unwrap();

    let mut spec = FnSpec {
        name: "test".to_string(),
        args: vec![FnArg {
            pos: 0,
            name: "id".to_string(),
            ty: Some(syn::parse_str("UserId").unwrap()),
            description: None,
        }],
        returns: vec![],
        description: None,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    #[derive(Default, darling::FromMeta)]
    struct EmptyConf {}

    let result = parse_and_apply_overrides::<EmptyConf>(&items, &mut spec);
    assert!(result.is_ok());

    assert_eq!(spec.description.as_deref(), Some("Mixed test"));
    assert_eq!(spec.args[0].ty, Some(syn::parse_str("i64").unwrap()));
    assert_eq!(spec.returns.len(), 2);
    assert_eq!(spec.returns[0].ty, Some(syn::parse_str("User").unwrap()));
    assert_eq!(
        spec.returns[1].ty,
        Some(syn::parse_str("ErrorResponse").unwrap())
    );
}

// ============================================================================
// Patch building tests
// ============================================================================

#[test]
fn test_build_arg_patch() {
    let arg = FnArg {
        pos: 0,
        name: "id".to_string(),
        ty: Some(syn::parse_str("UserId").unwrap()),
        description: Some("User ID".to_string()),
    };

    let patch = build_arg_patch(&arg);
    let patch_str = patch.to_string();

    assert!(patch_str.contains("0"));
    assert!(patch_str.contains("id"));
    assert!(patch_str.contains("UserId"));
    assert!(patch_str.contains("User ID"));
}

#[test]
fn test_build_return_patch() {
    let ret = FnReturn {
        ty: Some(syn::parse_str("ErrorResponse").unwrap()),
        status: Some(404),
        description: Some("Not found".to_string()),
    };

    let patch = build_return_patch(&ret);
    let patch_str = patch.to_string();

    assert!(patch_str.contains("ErrorResponse"));
    assert!(patch_str.contains("404"));
    assert!(patch_str.contains("Not found"));
}

#[test]
fn test_build_patch_chain_complete() {
    let spec = FnSpec {
        name: "handler".to_string(),
        args: vec![
            FnArg {
                pos: 0,
                name: "id".to_string(),
                ty: Some(syn::parse_str("UserId").unwrap()),
                description: Some("User ID".to_string()),
            },
            FnArg {
                pos: 1,
                name: "name".to_string(),
                ty: Some(syn::parse_str("String").unwrap()),
                description: None,
            },
        ],
        returns: vec![FnReturn {
            ty: Some(syn::parse_str("User").unwrap()),
            status: Some(200),
            description: Some("Success".to_string()),
        }],
        description: Some("User handler".to_string()),
        operation_id: None,
        deprecated: false,
        method: false,
        receiver: None,
    };

    let patch = build_patch_chain(&spec);
    let patch_str = patch.to_string();

    assert!(patch_str.contains("User handler"));
    assert!(patch_str.contains("id"));
    assert!(patch_str.contains("UserId"));
    assert!(patch_str.contains("name"));
    assert!(patch_str.contains("User"));
    assert!(patch_str.contains("200"));
}

// ============================================================================
// Validation tests
// ============================================================================

#[test]
fn test_validate_arg_with_valid_pos_and_name() {
    let input: proc_macro2::TokenStream = quote! {
        fn handler(id: UserId, name: String) -> User {
            unimplemented!()
        }
    };

    let mut spec = extract_func_spec(&input, "test_macro").unwrap();

    let attrs = vec![darling::ast::NestedMeta::Meta(syn::Meta::List(
        syn::MetaList {
            path: syn::parse_quote!(arg),
            delimiter: syn::MacroDelimiter::Paren(Default::default()),
            tokens: quote! { pos = 0, name = "id", description = "User ID" },
        },
    ))];

    let mut arg_overrides = vec![];
    for item in &attrs {
        if let darling::ast::NestedMeta::Meta(syn::Meta::List(list)) = item
            && list.path.is_ident("arg")
        {
            let nested = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone()).unwrap();
            let arg_ovr = ArgOverride::from_list(&nested).unwrap();
            arg_overrides.push(arg_ovr);
        }
    }

    // Should succeed - position 0 matches name "id"
    let result = apply_arg_overrides(&mut spec, &arg_overrides);
    assert!(result.is_ok());
    assert_eq!(spec.args[0].description, Some("User ID".to_string()));
}

#[test]
fn test_validate_arg_with_invalid_position() {
    let input: proc_macro2::TokenStream = quote! {
        fn handler(id: UserId) -> User {
            unimplemented!()
        }
    };

    let mut spec = extract_func_spec(&input, "test_macro").unwrap();

    // Position 5 is out of bounds (only 1 argument)
    let arg_ovr = ArgOverride {
        pos: Some(5),
        name: "id".to_string(),
        ty: None,
        description: Some("Invalid".to_string()),
    };

    let result = apply_arg_overrides(&mut spec, &[arg_ovr]);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("position") && err_msg.contains("5"));
}

#[test]
fn test_validate_arg_with_invalid_name() {
    let input: proc_macro2::TokenStream = quote! {
        fn handler(id: UserId, name: String) -> User {
            unimplemented!()
        }
    };

    let mut spec = extract_func_spec(&input, "test_macro").unwrap();

    // "nonexistent" doesn't match any argument
    let arg_ovr = ArgOverride {
        pos: None,
        name: "nonexistent".to_string(),
        ty: None,
        description: Some("Invalid".to_string()),
    };

    let result = apply_arg_overrides(&mut spec, &[arg_ovr]);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("nonexistent") && err_msg.contains("not found"));
}

#[test]
fn test_validate_arg_with_mismatched_pos_and_name() {
    let input: proc_macro2::TokenStream = quote! {
        fn handler(id: UserId, name: String) -> User {
            unimplemented!()
        }
    };

    let mut spec = extract_func_spec(&input, "test_macro").unwrap();

    // Position 0 is "id" but we're saying it's "name" - mismatch!
    let arg_ovr = ArgOverride {
        pos: Some(0),
        name: "name".to_string(),
        ty: None,
        description: Some("Mismatched".to_string()),
    };

    let result = apply_arg_overrides(&mut spec, &[arg_ovr]);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("mismatch"));
    assert!(err_msg.contains("id"));
    assert!(err_msg.contains("name"));
}

#[test]
fn test_validate_multiple_arg_overrides_with_errors() {
    let input: proc_macro2::TokenStream = quote! {
        fn handler(id: UserId, name: String) -> User {
            unimplemented!()
        }
    };

    let mut spec = extract_func_spec(&input, "test_macro").unwrap();

    // First override is valid, second has invalid position
    let overrides = vec![
        ArgOverride {
            pos: Some(0),
            name: "id".to_string(),
            ty: None,
            description: Some("Valid".to_string()),
        },
        ArgOverride {
            pos: Some(10),
            name: "invalid".to_string(),
            ty: None,
            description: Some("Invalid".to_string()),
        },
    ];

    let result = apply_arg_overrides(&mut spec, &overrides);
    assert!(result.is_err());
    // First override should have been applied before error
    assert_eq!(spec.args[0].description, Some("Valid".to_string()));
}
