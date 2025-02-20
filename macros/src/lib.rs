//! Procedural macros used through the rest of the project.
//! Because of the way proc macros work this has to be a separate crate.

use std::str::FromStr;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_crate::FoundCrate;
use quote::{format_ident, quote, ToTokens};
use syn::{
    bracketed,
    meta::ParseNestedMeta,
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    punctuated::Punctuated,
    Constraint, Data, DeriveInput, Expr, ExprLit, Fields, GenericArgument, Ident, Lit, Meta,
    MetaNameValue, Path, PathArguments, Token, Type, TypePath,
};

fn resolve_crate_name() -> TokenStream2 {
    match proc_macro_crate::crate_name("gremlin") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(#ident)
        }
        Err(_) => panic!("Could not find crate `gremlin`"),
    }
}

/// The `HasSchema` derive marco,
/// implementing [`HasSchema`](../gremlin/docs/trait.HasSchema.html),
/// to automatically produce documentation about the expected
/// CSV schema for deserializing to this struct.
#[proc_macro_derive(HasSchema, attributes(serde))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
    let gremlin = resolve_crate_name();
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let fields = match input.data {
        syn::Data::Struct(s) => s.fields,
        _ => {
            panic!("HasSchema can only be derived for structs")
        }
    };

    let schema_fields = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap().to_string();
        let field_type = &f.ty;
        let attrs = &f.attrs;

        let docs = attrs.iter().filter_map(|attr| {
            // Check if the attribute is a doc comment
            if attr.path().is_ident("doc") {
                if let Meta::NameValue(meta) = &attr.meta {
                    if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = &meta.value {
                        return Some(lit_str.value());
                    }
                }
            }
            None
        }).collect::<Vec<String>>().join("\n");

        let mut is_flattened = false;
        for attr in attrs.iter() {
            if attr.path().is_ident("serde") {
                if let Meta::Path(path) = &attr.meta {
                    if path.is_ident("flatten") {
                        is_flattened = true;
                    }
                }
            }
        }

        if is_flattened {
            quote! {
                <#field_type as #gremlin::docs::HasSchema>::schema().into_iter().map(|(name, typ, f_docs)| {
                    (format!("{}.{}", #field_name, name), typ, format!("{} : {}", #docs.trim().trim_right_matches(".").to_string(), f_docs))
                }).collect::<Vec<_>>()
            }
        } else {
            quote! {
                vec![(#field_name.to_string(), stringify!(#field_type), #docs.trim().to_string())]
            }
        }
    })
    .collect::<Vec<TokenStream2>>();

    let expanded = quote! {
        impl #gremlin::docs::HasSchema for #name {
            fn schema() -> Vec<(String, &'static str, String)> {
                vec![
                    #(#schema_fields),*
                ].into_iter().flatten()
                .collect::<Vec<_>>()
            }
        }
    };

    TokenStream::from(expanded)
}

fn is_vec_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        // Check if the type is Vec<_>
        type_path
            .path
            .segments
            .iter()
            .any(|segment| segment.ident == "Vec")
    } else {
        false
    }
}

/// You can use the struct-level attribute:
/// `#[row(MyStruct::some_method)]`
/// Note that this needs to return a type `T`
/// that implements `AsRowValue`.
#[proc_macro_derive(Row, attributes(row, facet))]
pub fn derive_row(input: TokenStream) -> TokenStream {
    let gremlin = resolve_crate_name();
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let mut facet = quote! { String::new() };
    let mut synthetic_rows = vec![];
    for attr in &input.attrs {
        if attr.path().is_ident("row") {
            if matches!(attr.meta, Meta::List(_)) {
                attr.parse_nested_meta(|meta| {
                    synthetic_rows.push(meta.path.clone());
                    Ok(())
                })
                .unwrap();
            }
        }
        if attr.path().is_ident("facet") {
            if matches!(attr.meta, Meta::List(_)) {
                attr.parse_nested_meta(|meta| {
                    let path = meta.path.clone();
                    facet = quote! { #path(self).to_string() };
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    let mut columns = Vec::new();
    let mut values = Vec::new();

    if let Data::Struct(data_struct) = input.data {
        if let Fields::Named(fields) = data_struct.fields {
            for field in fields.named {
                let field_name = field.ident.unwrap();
                let field_name_str = field_name.to_string();

                // Check for #[row] attribute
                let row_field = find_row_attribute(&field_name_str, &field.attrs);

                if let Some(row_field) = row_field {
                    // Handle nested Row types
                    let field_type = &field.ty;
                    columns.push(quote! {
                        <#field_type as #gremlin::data::Row>::columns()
                            .into_iter()
                            .map(|col| if col.is_empty() {
                                format!("{}", #row_field)
                            } else {
                                format!("{}.{}", #row_field, col)
                            })
                    });
                    values.push(quote! {
                        <#field_type as #gremlin::data::Row>::values(&self.#field_name)
                    });
                }

                // Check for #[facet] attribute
                let is_facet_field = field.attrs.iter().any(|attr| attr.path().is_ident("facet"));
                if is_facet_field {
                    facet = quote! { self.#field_name.to_string() };
                }
            }
        }
    }

    if !synthetic_rows.is_empty() {
        let names: Vec<_> = synthetic_rows
            .iter()
            .map(|func| func.segments.last().unwrap().ident.to_string())
            .collect();
        columns.push(quote! {
            vec![
                #(#names.to_string(),)*
            ]
        });
        values.push(quote! {
            vec![
                #(#synthetic_rows(self).as_f32(),)*
            ]
        });
    }

    // Generate the impl
    let expanded = quote! {
        impl #gremlin::data::Row for #struct_name {
            fn columns() -> Vec<String> {
                let mut cols = Vec::new();
                #(std::iter::Extend::extend(&mut cols, #columns);)*
                cols
            }

            fn values(&self) -> Vec<f32> {
                let mut vals = Vec::new();
                #(std::iter::Extend::extend(&mut vals, #values);)*
                vals
            }

            fn facet(&self) -> String {
                #facet
            }
        }
    };

    TokenStream::from(expanded)
}

/// Implements [`FromPartial`](../infra_lib/partial/trait.FromPartial.html) and other auxiliary things.
///
#[proc_macro_derive(Partial, attributes(partial))]
pub fn partial_struct(input: TokenStream) -> TokenStream {
    let gremlin = resolve_crate_name();
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident; // Original struct name
    let partial_name = format!("Partial{}", name); // Partial<Name> name
    let partial_ident = syn::Ident::new(&partial_name, name.span()); // Create an identifier for the new name

    let mut derive_partial_row = false;
    let mut derive_partial_constrained = false;
    let mut derive_from_default = false;
    for attr in &input.attrs {
        if attr.path().is_ident("partial") {
            attr.parse_nested_meta(|meta| {
                let ParseNestedMeta { path, .. } = &meta;
                if path.is_ident("row") {
                    derive_partial_row = true;
                } else if path.is_ident("constrained") {
                    derive_partial_constrained = true;
                } else if path.is_ident("from_default") {
                    derive_from_default = true;
                }
                Ok(())
            })
            .unwrap();
        }
    }

    let mut field_names = vec![];
    let mut field_idents = vec![];
    let mut field_types = vec![];
    let mut field_take_idents = vec![];
    let mut field_partial_to_full = vec![];
    let mut field_full_to_partial = vec![];
    let mut field_partial_to_full_unwrap = vec![];
    let mut row_fields = vec![];
    let mut field_constraints = vec![];
    let mut facet = quote! { String::new() };
    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for f in &fields.named {
                let name = f.ident.as_ref().unwrap().to_string();
                let ident = f.ident.as_ref().unwrap();
                let take_ident = syn::Ident::new(&format!("take_{}", ident), ident.span());
                let mut new_ty = f.ty.clone();

                let is_row_field = f.attrs.iter().any(|attr| attr.path().is_ident("row"));
                if is_row_field {
                    let field_type = &f.ty;
                    row_fields.push(quote! {
                        self.#ident.as_ref().map_or_else(|| {
                            <#field_type as #gremlin::data::Row>::columns()
                                .iter().map(|_| f32::NAN).collect()
                        }, <#field_type as #gremlin::data::Row>::values)
                    });
                }

                if derive_partial_constrained {
                    let mut constraints: Vec<_> = f
                        .attrs
                        .iter()
                        .filter(|attr| attr.path().is_ident("constraint"))
                        .map(|attr| attr.to_token_stream())
                        .collect();
                    if !constraints.is_empty() {
                        constraints.push(quote! { #[constraint(allow_none)] });
                    }
                    field_constraints.push(constraints);
                } else {
                    field_constraints.push(vec![]);
                }

                // Here we know the field will always be an `Option`.
                let is_facet_field = f.attrs.iter().any(|attr| attr.path().is_ident("facet"));
                if is_facet_field {
                    facet = quote! { self.#ident.map(|f| f.to_string()).unwrap_or_default() };
                }

                // Handle `#[partial]` attribute.
                let is_partial = f.attrs.iter().any(|attr| attr.path().is_ident("partial"));
                if is_partial {
                    if let Type::Path(TypePath { path, .. }) = &new_ty {
                        if path.segments.len() == 1 && path.segments[0].ident == "Vec" {
                            // The field is of type Vec<T>
                            // WARN: Note that you must implement `From<PartialT> for T`
                            // for this to work!
                            // One way to do this is to implement `Default for T`
                            // and then use `#[partial(from_default)]`
                            // when deriving `Partial` for `T`.
                            let segment = &path.segments[0].arguments;
                            if let syn::PathArguments::AngleBracketed(args) = segment {
                                if let Some(syn::GenericArgument::Type(inner_ty)) =
                                    args.args.first()
                                {
                                    // Modify the type to Vec<PartialT>
                                    let p_name = format!(
                                        "Vec<Partial{}>",
                                        inner_ty.to_token_stream().to_string()
                                    );
                                    new_ty = syn::parse_str::<Type>(&p_name).unwrap();
                                }
                            } else {
                                // PartialT
                                let p_name =
                                    format!("Partial{}", new_ty.to_token_stream().to_string());
                                new_ty = syn::parse_str::<Type>(&p_name).unwrap();
                            }
                        }
                    }
                }

                // Going from the partial Option<T> to T.
                let partial_to_full = if is_vec_type(&new_ty) {
                    // If it's a Vec, generate code for map(Into::into)
                    quote! {
                        partial.#take_ident()?.into_iter().map(Into::into).collect()
                    }
                } else {
                    // Otherwise, generate code for .into()
                    quote! {
                        partial.#take_ident()?.into()
                    }
                };

                let partial_to_full_unwrap = if is_vec_type(&new_ty) {
                    // If it's a Vec, generate code for map(Into::into)
                    quote! {
                        // SAFETY: Checked that the field is Some.
                        partial.#take_ident().unwrap().into_iter().map(Into::into).collect()
                    }
                } else {
                    // Otherwise, generate code for .into()
                    quote! {
                        // SAFETY: Checked that the field is Some.
                        partial.#take_ident().unwrap().into()
                    }
                };

                // Going from the full T to Option<T>.
                let full_to_partial = if is_vec_type(&new_ty) {
                    // If it's a Vec, generate code for map(Into::into)
                    quote! {
                        full.#ident.into_iter().map(Into::into).collect()
                    }
                } else {
                    // Otherwise, generate code for .into()
                    quote! {
                        full.#ident.into()
                    }
                };

                field_names.push(name);
                field_idents.push(ident);
                field_types.push(new_ty);
                field_take_idents.push(take_ident);
                field_partial_to_full.push(partial_to_full);
                field_full_to_partial.push(full_to_partial);
                field_partial_to_full_unwrap.push(partial_to_full_unwrap);
            }
        } else {
            panic!("PartialStruct macro only supports named fields.");
        }
    } else {
        panic!("PartialStruct macro can only be used with structs.");
    }

    let row_impl = if derive_partial_row {
        quote! {
            impl #gremlin::data::Row for #partial_ident {
                fn columns() -> Vec<String> {
                    #name::columns()
                }

                fn values(&self) -> Vec<f32> {
                    let mut vals = Vec::new();
                    #(std::iter::Extend::extend(&mut vals, #row_fields);)*
                    vals
                }

                fn facet(&self) -> String {
                    #facet
                }
            }
        }
    } else {
        quote! {}
    };

    let constrained_derive = if derive_partial_constrained {
        quote! { Constrained, }
    } else {
        quote! {}
    };

    let from_default_impl = if derive_from_default {
        quote! {
            impl From<#partial_ident> for #name {
                fn from(partial: #partial_ident) -> Self {
                    let mut default = Self::default();
                    default.apply(partial);
                    default
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, #constrained_derive)]
        pub struct #partial_ident {
            #(#(#field_constraints)*
              pub #field_idents: std::option::Option<#field_types>,)*
        }

        impl #partial_ident {
            #(pub fn #field_idents(&self) -> Result<&#field_types, #gremlin::data::HydrateError> {
                self.#field_idents.as_ref().ok_or(#gremlin::data::HydrateError::MissingExpectedField {
                    name: stringify!(#name),
                    field: stringify!(#field_idents),
                    backtrace: std::backtrace::Backtrace::force_capture(),
                })
            })*

            #(pub fn #field_take_idents(&mut self) -> Result<#field_types, #gremlin::data::HydrateError> {
                self.#field_idents.take().ok_or(#gremlin::data::HydrateError::MissingExpectedField {
                    name: stringify!(#name),
                    field: stringify!(#field_idents),
                    backtrace: std::backtrace::Backtrace::force_capture(),
                })
            })*
        }

        impl #gremlin::data::Partial for #partial_ident {
            fn missing_fields(&self) -> Vec<&'static str> {
                let mut missing = vec![];
                #(if self.#field_idents.is_none() {
                    missing.push(stringify!(#field_idents));
                })*
                missing
            }
        }

        #row_impl
        #from_default_impl

        impl Default for #partial_ident {
            fn default() -> Self {
                Self {
                    #(#field_idents: None,)*
                }
            }
        }

        impl From<#name> for #partial_ident {
            fn from(full: #name) -> Self {
                let mut partial = Self::default();
                #(
                    partial.#field_idents = Some(#field_full_to_partial);
                )*
                partial
            }
        }

        impl #gremlin::data::FromPartial for #name {
            type Partial = #partial_ident;

            fn from(mut partial: Self::Partial) -> Result<Self, #gremlin::data::HydrateError> {
                let mut missing_fields = Vec::new();

                #(
                    if partial.#field_idents.is_none() {
                        missing_fields.push(stringify!(#field_idents));
                    }
                )*

                if !missing_fields.is_empty() {
                    return Err(#gremlin::data::HydrateError::EmptyFields(stringify!(#name), missing_fields));
                }

                Ok(#name {
                    #(#field_idents: #field_partial_to_full,)*
                })
            }

            fn apply(&mut self, mut partial: Self::Partial) {
                #(
                    if partial.#field_idents.is_some() {
                        self.#field_idents = #field_partial_to_full_unwrap;
                    }
                )*
            }
        }
    };

    TokenStream::from(expanded)
}

/// Find a specified alternate name for the row;
/// defaults to the field name if none is found.
fn find_row_attribute(field: &str, attrs: &[syn::Attribute]) -> Option<String> {
    let mut found = None;
    for attr in attrs {
        if attr.path().is_ident("row") {
            if let Meta::NameValue(meta) = &attr.meta {
                if meta.path.is_ident("rename") {
                    if let Expr::Lit(ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }) = &meta.value
                    {
                        found = Some(lit_str.value());
                    }
                }
            }
            if found.is_none() {
                found = Some(field.to_string());
            }
        }
    }
    found
}

struct MacroInput {
    types: Punctuated<Type, Token![,]>,
    user_macro: Path,
}

impl Parse for MacroInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        bracketed!(content in input);
        let types = Punctuated::<Type, Token![,]>::parse_terminated(&content)?;
        let user_macro: Path = input.parse()?;
        Ok(MacroInput { types, user_macro })
    }
}

/// Given a list of identifiers and a macro,
/// this iterates over the self-Cartesian product
/// of the identifiers, excluding the identity pairs.
///
/// For example, with the identifiers `[A, B, C]`
/// this yields the pairs:
///
/// ```
/// (A, B), (A, C), (B, A), (B, C), (C, A), (C, B)
/// ```
///
/// i.e. it skips `(A, A), (B, B), (C, C)`.
///
/// Then each pair is passed into the provided macro.
///
/// For example:
///
/// ```
/// non_identity_pairs!([A, B, C], my_macro);
/// ```
///
/// Would call `my_macro!(A, B)`, `my_macro!(A, C)`, etc.
#[doc(hidden)]
#[proc_macro]
pub fn non_identity_pairs(input: TokenStream) -> TokenStream {
    let MacroInput { types, user_macro } = parse_macro_input!(input as MacroInput);

    let extracted: Vec<_> = types.into_iter().map(|ty| extract_generics(&ty)).collect();
    let mut blocks = proc_macro2::TokenStream::new();
    for (x, x_constraints) in &extracted {
        for (y, y_constraints) in &extracted {
            // There's probably a better way to do this,
            // but join the constraints, e.g.
            // if we have `FP: Prefix` and `GP: Prefix`,
            // this will join into `FP: Prefix, GP: Prefix`.
            let constraints: Vec<_> = x_constraints
                .iter()
                .chain(y_constraints.iter())
                .map(|con| con.to_token_stream().to_string())
                .collect();
            let constraints = proc_macro2::TokenStream::from_str(&constraints.join(",")).unwrap();

            // Only implement when `X != Y`.
            if x.to_token_stream().to_string() != y.to_token_stream().to_string() {
                let expanded = quote! {
                    #user_macro!(#x, #y, #constraints);
                };
                blocks.extend(expanded);
            }
        }
    }

    blocks.into()
}

/// Extract generic type constraints, if any,
/// and modify the generic names to avoid potential clashes.
fn extract_generics(ty: &Type) -> (Type, Option<Constraint>) {
    let mut ret_ty = ty.clone();
    let mut constr = None;
    if let Type::Path(TypePath { path, .. }) = ty {
        for segment in &path.segments {
            let base = &segment.ident;
            if let PathArguments::AngleBracketed(angle_bracketed) = &segment.arguments {
                for arg in &angle_bracketed.args {
                    if let GenericArgument::Constraint(constraint) = arg {
                        // To avoid name clashes, append the base type to the constraint types,
                        // e.g. `Foo<P: Bar>` changes `P` to `FooP`.
                        let generic = format_ident!("{}{}", base, constraint.ident);

                        // Create the new type that we'll actually use,
                        // e.g. `Foo<P: Bar>` becomes `Foo<FooP>`.
                        ret_ty = syn::parse_str::<Type>(&format!("{}<{}>", base, generic))
                            .expect("Failed to parse type with generics");
                        // let bounds = constraint.bounds.to_token_stream().to_string();
                        // let bounds = format!("{}: {}", generic, bounds);

                        // Modify the constraint to match,
                        // e.g. `P: Bar` becomes `FooP: Bar`.
                        let mut constraint = constraint.clone();
                        constraint.ident = generic;
                        constr = Some(constraint);
                        // // let bounds = syn::parse_str::<TypeParamBound>(&bounds).expect("Failed to parse generic bounds");
                        // // println!("bounds {:?}", bounds.to_token_stream().to_string());
                        // let constraint_str = quote! { #constraint }.to_string();
                        // // let constraint_str = quote! { #new_ty }.to_string();
                        // println!("constraint_str {:?}", constraint_str);
                    }
                }
            }
        }
    }

    (ret_ty, constr)
}

/// Implements a `list` method for an enum
/// which iterates over its variant names and their associated docstrings.
#[proc_macro_derive(HasVariants, attributes(doc))]
pub fn has_variants_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    // Ensure we're dealing with an enum
    let name = &ast.ident;
    let data = if let Data::Enum(data) = ast.data {
        data
    } else {
        panic!("HasVariants only works on enums!");
    };

    // Extract enum variants and their docs
    let variants = data.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let docs = variant
            .attrs
            .iter()
            .filter_map(|attr| {
                if let Meta::NameValue(meta) = &attr.meta {
                    if let Expr::Lit(ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }) = &meta.value
                    {
                        return Some(lit_str.value());
                    }
                }
                None
            })
            .collect::<Vec<String>>()
            .join(" ");

        quote! {
            (String::from(stringify!(#variant_name)), String::from(#docs))
        }
    });

    // Implement the trait
    let gen = quote! {
        impl HasVariants for #name {
            fn describe_variants() -> Vec<(String, String)> {
                vec![#(#variants),*]
            }
        }
    };

    gen.into()
}

#[proc_macro_derive(Constrained, attributes(constraint, constrained))]
pub fn derive_constrained(input: TokenStream) -> TokenStream {
    let gremlin = resolve_crate_name();
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let fields = match input.data {
        syn::Data::Struct(data) => data.fields,
        _ => panic!("Constrained can only be derived for structs"),
    };

    let mut field_validations = vec![];

    for field in fields {
        let field_name = field.ident.unwrap();
        let mut constraints = vec![];

        // TODO: This can be handled better;
        // the problem is basically:
        // when deriving a `Partial` type
        // all the fields become e.g. `Option<f32>`
        // instead of `f32`, and we want to allow
        // `None` values as "valid" because there isn't
        // yet any value to check.
        // As implemented now, the presence of
        // `#[constrain(allow_none)]` will do this,
        // and assumes that the field is in fact `Option<f32>`.
        let mut allow_none = false;
        for attr in &field.attrs {
            if attr.path().is_ident("constraint") {
                if matches!(attr.meta, Meta::List(_)) {
                    let _ = attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("allow_none") {
                            allow_none = true;
                        }
                        Ok(())
                    });
                }
            }
        }

        for attr in field.attrs {
            if attr.path().is_ident("constraint") {
                match attr.meta {
                    Meta::List(list) => {
                        if let Ok(meta) = syn::parse2::<MetaNameValue>(list.tokens) {
                            let constraint = meta.path.get_ident().unwrap();
                            let value = meta.value;
                            constraints
                                .push(quote! { #gremlin::data::Constraint::#constraint(#value) });
                        }
                    }
                    Meta::Path(path) => {
                        if path.is_ident("allow_none") {
                            // `allow_none` handled above.
                            continue;
                        }

                        let function = path.get_ident().unwrap();

                        // Allow `None` values to pass.
                        if allow_none {
                            field_validations.push(quote! {
                                if let Some(value) = &self.#field_name {
                                    if !#function(value) {
                                        errors.push(#gremlin::data::Breach {
                                            field: stringify!(#field_name).to_string(),
                                            constraint: stringify!(#function).to_string(),
                                            value: value.to_string(),
                                        })
                                    }
                                }
                            });
                        } else {
                            field_validations.push(quote! {
                                if !#function(&self.#field_name) {
                                    errors.push(#gremlin::data::Breach {
                                        field: stringify!(#field_name).to_string(),
                                        constraint: stringify!(#function).to_string(),
                                        value: self.#field_name.to_string(),
                                    })
                                }
                            });
                        }
                    }
                    Meta::NameValue(meta) => {
                        let constraint = meta.path.get_ident().unwrap();
                        let value = meta.value;
                        constraints
                            .push(quote! { #gremlin::data::Constraint::#constraint(#value) });
                    }
                }
            } else if attr.path().is_ident("constrained") {
                // Allow `None` values to pass.
                if allow_none {
                    field_validations.push(quote! {
                        if let Some(value) = &self.#field_name {
                            for mut err in #gremlin::data::Constrained::validate(value) {
                                err.field = format!("{}.{}", stringify!(#field_name), err.field);
                                errors.push(err);
                            }
                        }
                    });
                } else {
                    field_validations.push(quote! {
                        for mut err in #gremlin::data::Constrained::validate(&self.#field_name) {
                            err.field = format!("{}.{}", stringify!(#field_name), err.field);
                            errors.push(err);
                        }
                    });
                }
            }
        }

        if !constraints.is_empty() {
            // Allow `None` values to pass.
            if allow_none {
                field_validations.push(quote! {
                    if let Some(value) = &self.#field_name {
                        for c in [#(#constraints),*] {
                            if !c.validate(f32::from(*value)) {
                                errors.push(#gremlin::data::Breach {
                                    field: stringify!(#field_name).to_string(),
                                    constraint: c.to_string(),
                                    value: value.to_string(),
                                })
                            }
                        }
                    }
                });
            } else {
                field_validations.push(quote! {
                    for c in [#(#constraints),*] {
                        if !c.validate(f32::from(self.#field_name)) {
                            errors.push(#gremlin::data::Breach {
                                field: stringify!(#field_name).to_string(),
                                constraint: c.to_string(),
                                value: self.#field_name.to_string(),
                            })
                        }
                    }
                });
            }
        }
    }

    let expanded = quote! {
        impl #gremlin::data::Constrained for #struct_name {
            fn validate(&self) -> Vec<#gremlin::data::Breach> {
                let mut errors = vec![];
                #(#field_validations)*
                errors
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Dataset, attributes(dataset))]
pub fn derive_dataset(input: TokenStream) -> TokenStream {
    let gremlin = resolve_crate_name();
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let mut row_type = None;
    let mut facet_type = None;

    for attr in &input.attrs {
        if attr.path().is_ident("dataset") {
            if let Meta::List(_) = attr.meta {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("row") {
                        let value = meta.value().unwrap();
                        let path: Ident = value.parse().unwrap();
                        row_type = Some(path);
                    } else if meta.path.is_ident("facet") {
                        let value = meta.value().unwrap();
                        let path: Ident = value.parse().unwrap();
                        facet_type = Some(path);
                    }
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    let mut rows_field = None;

    if let Data::Struct(data_struct) = input.data {
        if let Fields::Named(fields) = data_struct.fields {
            for field in fields.named {
                for attr in &field.attrs {
                    if attr.path().is_ident("dataset") {
                        attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("rows") {
                                rows_field = Some(field.ident.clone().unwrap());
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                }
            }
        }
    }

    let expanded = match facet_type {
        Some(facet_type) => {
            quote! {
                impl #gremlin::data::Dataset for #struct_name {
                    type Row = #row_type;
                    type Facet = #facet_type;

                    fn rows(&self) -> impl Iterator<Item = &Self::Row> {
                        self.#rows_field.iter()
                    }

                    fn faceted(&self) -> std::collections::BTreeMap<Self::Facet, Vec<&Self::Row>> {
                        use #gremlin::data::Rows;
                        let hm = self
                            .rows()
                            .group_by(|row| self.get_row_facet(row));
                        hm.into_iter().collect()
                    }
                }
            }
        }
        None => {
            quote! {
                impl #gremlin::data::Dataset for #struct_name {
                    type Row = #row_type;
                    type Facet = String;

                    fn rows(&self) -> impl Iterator<Item = &Self::Row> {
                        self.#rows_field.iter()
                    }

                    fn faceted(&self) -> std::collections::BTreeMap<Self::Facet, Vec<&Self::Row>> {
                        Default::default()
                    }
                }
            }
        }
    };

    TokenStream::from(expanded)
}
