//! Procedural macros used through the rest of the project.
//! Because of the way proc macros work this has to be a separate crate.

use std::str::FromStr;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens};
use syn::{
    bracketed,
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    punctuated::Punctuated,
    Constraint, Data, DeriveInput, Fields, GenericArgument, Lit, Meta, NestedMeta, Path,
    PathArguments, Token, Type, TypePath,
};

/// The `HasSchema` derive marco,
/// implementing [`HasSchema`](../gremlin/docs/trait.HasSchema.html),
/// to automatically produce documentation about the expected
/// CSV schema for deserializing to this struct.
#[proc_macro_derive(HasSchema, attributes(serde))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
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
            if attr.path.is_ident("doc") {
                if let Ok(Meta::NameValue(meta_name_value)) = attr.parse_meta() {
                    if let Lit::Str(lit_str) = &meta_name_value.lit {
                        return Some(lit_str.value());
                    }
                }
            }
            None
        }).collect::<Vec<String>>().join("\n");

        let is_flattened = attrs.iter()
            .any(|attr| {
                matches!(attr.parse_meta(), Ok(Meta::List(meta)) if meta.path.is_ident("serde") && meta.nested.iter().any(|n| matches!(n, NestedMeta::Meta(Meta::Path(path)) if path.is_ident("flatten"))))
            });

        if is_flattened {
            quote! {
                <#field_type as HasSchema>::schema().into_iter().map(|(name, typ, f_docs)| {
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
        impl HasSchema for #name {
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

/// Implements [`FromPartial`](../infra_lib/partial/trait.FromPartial.html) and other auxiliary things.
///
#[proc_macro_derive(Partial, attributes(partial))]
pub fn partial_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident; // Original struct name
    let partial_name = format!("Partial{}", name); // Partial<Name> name
    let partial_ident = syn::Ident::new(&partial_name, name.span()); // Create an identifier for the new name

    let mut field_names = vec![];
    let mut field_idents = vec![];
    let mut field_types = vec![];
    let mut field_conversions = vec![];
    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for f in &fields.named {
                let name = f.ident.as_ref().unwrap().to_string();
                let ident = f.ident.as_ref().unwrap();
                let mut new_ty = f.ty.clone();

                // Handle `#[partial]` attribute.
                let is_partial = f.attrs.iter().any(|attr| attr.path.is_ident("partial"));
                if is_partial {
                    if let Type::Path(TypePath { path, .. }) = &new_ty {
                        if path.segments.len() == 1 && path.segments[0].ident == "Vec" {
                            // The field is of type Vec<T>
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

                let conversion = if is_vec_type(&new_ty) {
                    // If it's a Vec, generate code for map(Into::into)
                    quote! {
                        partial.#ident.unwrap().into_iter().map(Into::into).collect()
                    }
                } else {
                    // Otherwise, generate code for .into()
                    quote! {
                        partial.#ident.unwrap().into()
                    }
                };

                field_names.push(name);
                field_idents.push(ident);
                field_types.push(new_ty);
                field_conversions.push(conversion);
            }
        } else {
            panic!("PartialStruct macro only supports named fields.");
        }
    } else {
        panic!("PartialStruct macro can only be used with structs.");
    }

    let expanded = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #partial_ident {
            #(pub #field_idents: std::option::Option<#field_types>,)*
        }

        impl #partial_ident {
            #(pub fn #field_idents(&self) -> Result<&#field_types, HydrateError> {
                self.#field_idents.as_ref().ok_or(HydrateError::MissingExpectedField(stringify!(#name), stringify!(#field_idents)))
            })*
        }

        impl Default for #partial_ident {
            fn default() -> Self {
                Self {
                    #(#field_idents: None,)*
                }
            }
        }

        impl From<#partial_ident> for #name {
            fn from(value: #partial_ident) -> Self {
                <Self as FromPartial>::from_default(value)
            }
        }

        impl FromPartial for #name {
            type Partial = #partial_ident;

            fn from(partial: Self::Partial) -> Result<Self, HydrateError> {
                let mut missing_fields = Vec::new();

                #(
                    if partial.#field_idents.is_none() {
                        missing_fields.push(stringify!(#field_idents));
                    }
                )*

                if !missing_fields.is_empty() {
                    return Err(HydrateError::EmptyFields(stringify!(#name), missing_fields));
                }

                Ok(#name {
                    #(#field_idents: #field_conversions,)*
                })
            }

            fn into_partial(self) -> Self::Partial {
                let mut partial = Self::Partial::default();
                #(
                    partial.#field_idents = Some(self.#field_idents);
                )*
                partial
            }

            fn apply(&mut self, partial: Self::Partial) {
                #(
                    if partial.#field_idents.is_some() {
                        self.#field_idents = #field_conversions;
                    }
                )*
            }

            fn from_default(partial: Self::Partial) -> Self {
                let mut default = Self::default();
                default.apply(partial);
                default
            }
        }
    };

    TokenStream::from(expanded)
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
                if let Ok(Meta::NameValue(meta)) = attr.parse_meta() {
                    if meta.path.is_ident("doc") {
                        if let syn::Lit::Str(lit) = meta.lit {
                            return Some(lit.value());
                        }
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
